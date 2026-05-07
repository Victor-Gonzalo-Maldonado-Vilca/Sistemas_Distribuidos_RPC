import threading
import os
from time import sleep

from flask import Flask, jsonify
import amqpstorm
from amqpstorm import Message

app = Flask(__name__)

# ── URL de CloudAMQP ────────────────────────────────────────────────────────
URL_NUBE = "amqps://vdhlnbov:ogNW_b6xOVvycixvXj2uyAJOpWgCbmkx@chameleon.lmq.cloudamqp.com/vdhlnbov"


class RpcClient(object):
    """Cliente RPC Asíncrono con reconexión automática."""

    def __init__(self, rpc_queue):
        self.queue = {}
        self.rpc_queue = rpc_queue
        self.channel = None
        self.connection = None
        self.callback_queue = None
        self._lock = threading.Lock()
        self.open()

    def open(self):
        print("[Flask-RPC] Conectando a CloudAMQP...")
        self.connection = amqpstorm.UriConnection(URL_NUBE)
        self.channel = self.connection.channel()
        self.channel.queue.declare(self.rpc_queue)
        result = self.channel.queue.declare(exclusive=True)
        self.callback_queue = result['queue']
        self.channel.basic.consume(
            self._on_response,
            no_ack=True,
            queue=self.callback_queue
        )
        self._create_process_thread()
        print(f"[Flask-RPC] Conectado. Cola de respuestas: {self.callback_queue}")

    def _create_process_thread(self):
        thread = threading.Thread(target=self._process_data_events)
        thread.daemon = True
        thread.start()

    def _process_data_events(self):
        self.channel.start_consuming(to_tuple=False)

    def _on_response(self, message):
        with self._lock:
            self.queue[message.correlation_id] = message.body

    def send_request(self, payload):
        message = Message.create(self.channel, payload)
        message.reply_to = self.callback_queue
        with self._lock:
            self.queue[message.correlation_id] = None
        message.publish(routing_key=self.rpc_queue)
        print(f"[Flask-RPC] Mensaje enviado. corr_id={message.correlation_id}")
        return message.correlation_id


# ── Inicialización LAZY del cliente RPC ────────────────────────────────────
_rpc_client = None
_rpc_lock = threading.Lock()

def get_rpc_client():
    global _rpc_client
    if _rpc_client is None:
        with _rpc_lock:
            if _rpc_client is None:
                _rpc_client = RpcClient('rpc_queue')
    return _rpc_client


# ── Rutas Flask ─────────────────────────────────────────────────────────────

@app.route('/')
def index():
    html = """<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Sistema RPC Distribuido — Laboratorio de SD</title>
  <link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=Libre+Baskerville:ital,wght@0,400;0,700;1,400&display=swap" rel="stylesheet"/>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

    :root {
      --bg:       #0d0f14;
      --surface:  #13161e;
      --border:   #1e2330;
      --accent:   #4f8ef7;
      --accent2:  #7dd3a8;
      --danger:   #e06c75;
      --text:     #cdd6f4;
      --muted:    #6c7086;
      --mono:     'IBM Plex Mono', monospace;
      --serif:    'Libre Baskerville', Georgia, serif;
    }

    body {
      background: var(--bg);
      color: var(--text);
      font-family: var(--serif);
      min-height: 100vh;
      display: flex;
      flex-direction: column;
      align-items: center;
      padding: 3rem 1.5rem 5rem;
    }

    /* subtle grid overlay */
    body::before {
      content: '';
      position: fixed; inset: 0;
      background-image:
        linear-gradient(rgba(79,142,247,.04) 1px, transparent 1px),
        linear-gradient(90deg, rgba(79,142,247,.04) 1px, transparent 1px);
      background-size: 40px 40px;
      pointer-events: none;
    }

    header {
      width: 100%;
      max-width: 860px;
      border-bottom: 1px solid var(--border);
      padding-bottom: 1.5rem;
      margin-bottom: 2.5rem;
    }

    .institution {
      font-family: var(--mono);
      font-size: .72rem;
      letter-spacing: .18em;
      color: var(--muted);
      text-transform: uppercase;
      margin-bottom: .6rem;
    }

    h1 {
      font-size: 1.7rem;
      font-weight: 700;
      line-height: 1.25;
      letter-spacing: -.01em;
      color: #e6eaf7;
    }

    h1 span {
      color: var(--accent);
    }

    .subtitle {
      margin-top: .5rem;
      font-size: .9rem;
      color: var(--muted);
      font-style: italic;
    }

    .badge-row {
      display: flex;
      gap: .6rem;
      flex-wrap: wrap;
      margin-top: 1rem;
    }

    .badge {
      font-family: var(--mono);
      font-size: .68rem;
      letter-spacing: .08em;
      padding: .25rem .65rem;
      border-radius: 3px;
      border: 1px solid;
    }

    .badge-green  { color: var(--accent2); border-color: var(--accent2); background: rgba(125,211,168,.06); }
    .badge-blue   { color: var(--accent);  border-color: var(--accent);  background: rgba(79,142,247,.06);  }
    .badge-muted  { color: var(--muted);   border-color: var(--border);  background: transparent; }

    main { width: 100%; max-width: 860px; }

    /* ─── Card ─── */
    .card {
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 6px;
      padding: 1.75rem 2rem;
      margin-bottom: 1.5rem;
    }

    .card-title {
      font-family: var(--mono);
      font-size: .7rem;
      letter-spacing: .16em;
      color: var(--muted);
      text-transform: uppercase;
      margin-bottom: 1.1rem;
      display: flex;
      align-items: center;
      gap: .5rem;
    }

    .card-title::before {
      content: '';
      display: inline-block;
      width: 6px; height: 6px;
      border-radius: 50%;
      background: var(--accent);
    }

    /* ─── Input area ─── */
    .input-row {
      display: flex;
      gap: .75rem;
      align-items: stretch;
    }

    #payload-input {
      flex: 1;
      background: var(--bg);
      border: 1px solid var(--border);
      border-radius: 4px;
      color: var(--text);
      font-family: var(--mono);
      font-size: .9rem;
      padding: .75rem 1rem;
      outline: none;
      transition: border-color .2s;
    }

    #payload-input:focus {
      border-color: var(--accent);
    }

    #send-btn {
      background: var(--accent);
      border: none;
      border-radius: 4px;
      color: #fff;
      font-family: var(--mono);
      font-size: .82rem;
      font-weight: 600;
      letter-spacing: .06em;
      padding: .75rem 1.4rem;
      cursor: pointer;
      transition: opacity .2s, transform .1s;
      white-space: nowrap;
    }

    #send-btn:hover  { opacity: .85; }
    #send-btn:active { transform: scale(.97); }
    #send-btn:disabled { opacity: .4; cursor: not-allowed; }

    .hint {
      margin-top: .75rem;
      font-family: var(--mono);
      font-size: .75rem;
      color: var(--muted);
    }

    .hint code {
      color: var(--accent2);
    }

    /* ─── URL preview ─── */
    #url-preview {
      margin-top: .9rem;
      font-family: var(--mono);
      font-size: .78rem;
      color: var(--muted);
      word-break: break-all;
    }

    #url-preview span { color: var(--accent); }

    /* ─── Response panel ─── */
    #response-panel { display: none; }

    .resp-meta {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 1rem;
      flex-wrap: wrap;
      gap: .5rem;
    }

    .resp-status {
      font-family: var(--mono);
      font-size: .78rem;
      padding: .22rem .7rem;
      border-radius: 3px;
      border: 1px solid;
    }

    .resp-status.ok      { color: var(--accent2); border-color: var(--accent2); background: rgba(125,211,168,.08); }
    .resp-status.error   { color: var(--danger);  border-color: var(--danger);  background: rgba(224,108,117,.08); }
    .resp-status.pending { color: var(--accent);  border-color: var(--accent);  background: rgba(79,142,247,.08); }

    .resp-time {
      font-family: var(--mono);
      font-size: .72rem;
      color: var(--muted);
    }

    #response-body {
      background: var(--bg);
      border: 1px solid var(--border);
      border-radius: 4px;
      padding: 1.1rem 1.2rem;
      font-family: var(--mono);
      font-size: .88rem;
      color: var(--text);
      white-space: pre-wrap;
      word-break: break-word;
      min-height: 60px;
      line-height: 1.6;
    }

    /* ─── Architecture diagram ─── */
    .arch {
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 0;
      flex-wrap: wrap;
      margin: .5rem 0;
    }

    .arch-node {
      display: flex;
      flex-direction: column;
      align-items: center;
      gap: .35rem;
    }

    .arch-box {
      border: 1px solid var(--border);
      border-radius: 4px;
      padding: .55rem 1.1rem;
      font-family: var(--mono);
      font-size: .73rem;
      background: var(--bg);
      color: var(--accent);
      text-align: center;
      white-space: nowrap;
    }

    .arch-label {
      font-family: var(--mono);
      font-size: .62rem;
      color: var(--muted);
      letter-spacing: .06em;
      text-transform: uppercase;
    }

    .arch-arrow {
      font-size: 1.1rem;
      color: var(--muted);
      padding: 0 .4rem;
      margin-bottom: 1.2rem;
    }

    /* ─── Endpoint table ─── */
    table {
      width: 100%;
      border-collapse: collapse;
      font-family: var(--mono);
      font-size: .8rem;
    }

    th {
      text-align: left;
      color: var(--muted);
      font-weight: 400;
      padding: .5rem .75rem;
      border-bottom: 1px solid var(--border);
      letter-spacing: .08em;
    }

    td {
      padding: .6rem .75rem;
      border-bottom: 1px solid rgba(30,35,48,.6);
      vertical-align: top;
    }

    td:first-child { color: var(--accent2); }
    td:last-child  { color: var(--muted); }

    /* ─── Footer ─── */
    footer {
      margin-top: 3rem;
      font-family: var(--mono);
      font-size: .7rem;
      color: var(--muted);
      text-align: center;
      letter-spacing: .06em;
    }

    /* ─── Spinner ─── */
    @keyframes spin { to { transform: rotate(360deg); } }
    .spinner {
      display: inline-block;
      width: 12px; height: 12px;
      border: 2px solid rgba(79,142,247,.25);
      border-top-color: var(--accent);
      border-radius: 50%;
      animation: spin .7s linear infinite;
      vertical-align: middle;
      margin-right: .4rem;
    }
  </style>
</head>
<body>

<header>
  <p class="institution">Laboratorio de Sistemas Distribuidos &mdash; Arquitecturas de Mensajería</p>
  <h1>Sistema <span>RPC</span> sobre RabbitMQ</h1>
  <p class="subtitle">Demostración de llamada a procedimiento remoto mediante cola de mensajes asíncronos</p>
  <div class="badge-row">
    <span class="badge badge-green">● Servicio activo</span>
    <span class="badge badge-blue">CloudAMQP · AMQPS</span>
    <span class="badge badge-muted">Flask · AMQPStorm</span>
    <span class="badge badge-muted">Patrón: RPC sobre MQ</span>
  </div>
</header>

<main>

  <!-- ─── Diagrama de arquitectura ─── -->
  <div class="card">
    <p class="card-title">Arquitectura del sistema</p>
    <div class="arch">
      <div class="arch-node">
        <div class="arch-box">Cliente HTTP<br/>(Navegador / curl)</div>
        <div class="arch-label">Iniciador</div>
      </div>
      <div class="arch-arrow">→</div>
      <div class="arch-node">
        <div class="arch-box">Flask<br/>RPC Client</div>
        <div class="arch-label">Servidor Web</div>
      </div>
      <div class="arch-arrow">→</div>
      <div class="arch-node">
        <div class="arch-box">CloudAMQP<br/>rpc_queue</div>
        <div class="arch-label">Broker AMQP</div>
      </div>
      <div class="arch-arrow">→</div>
      <div class="arch-node">
        <div class="arch-box">Worker<br/>RPC</div>
        <div class="arch-label">Consumidor</div>
      </div>
    </div>
  </div>

  <!-- ─── Consola de prueba ─── -->
  <div class="card">
    <p class="card-title">Consola de invocación RPC</p>
    <div class="input-row">
      <input
        id="payload-input"
        type="text"
        placeholder="Ingrese el mensaje a enviar al Worker…"
        value="Hola_Desde_Laboratorio"
        autocomplete="off"
      />
      <button id="send-btn" onclick="sendRpc()">Enviar →</button>
    </div>
    <p class="hint">
      Endpoint: <code>/rpc_call/{payload}</code> &mdash;
      el Worker procesará el mensaje y retornará la respuesta vía cola de retorno.
    </p>
    <p id="url-preview"></p>
  </div>

  <!-- ─── Panel de respuesta ─── -->
  <div class="card" id="response-panel">
    <p class="card-title">Respuesta del Worker</p>
    <div class="resp-meta">
      <span class="resp-status pending" id="resp-status">Procesando…</span>
      <span class="resp-time" id="resp-time"></span>
    </div>
    <div id="response-body"></div>
  </div>

  <!-- ─── Tabla de endpoints ─── -->
  <div class="card">
    <p class="card-title">Endpoints disponibles</p>
    <table>
      <thead>
        <tr>
          <th>MÉTODO</th>
          <th>RUTA</th>
          <th>DESCRIPCIÓN</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td>GET</td>
          <td>/</td>
          <td>Panel de documentación y consola de prueba</td>
        </tr>
        <tr>
          <td>GET</td>
          <td>/rpc_call/{payload}</td>
          <td>Publica el mensaje en la cola RPC y retorna la respuesta del Worker</td>
        </tr>
      </tbody>
    </table>
  </div>

</main>

<footer>
  SISTEMAS DISTRIBUIDOS &nbsp;·&nbsp; ARQUITECTURAS DE MENSAJERÍA &nbsp;·&nbsp;
  PATRÓN RPC SOBRE AMQP
</footer>

<script>
  const input = document.getElementById('payload-input');
  const preview = document.getElementById('url-preview');

  function updatePreview() {
    const val = input.value.trim() || '{payload}';
    const origin = window.location.origin;
    preview.innerHTML = `URL generada: <span>${origin}/rpc_call/${encodeURIComponent(val)}</span>`;
  }

  input.addEventListener('input', updatePreview);
  updatePreview();

  // Enter key
  input.addEventListener('keydown', e => { if (e.key === 'Enter') sendRpc(); });

  async function sendRpc() {
    const payload = input.value.trim();
    if (!payload) { input.focus(); return; }

    const btn  = document.getElementById('send-btn');
    const panel = document.getElementById('response-panel');
    const statusEl = document.getElementById('resp-status');
    const timeEl   = document.getElementById('resp-time');
    const bodyEl   = document.getElementById('response-body');

    btn.disabled = true;
    panel.style.display = 'block';
    statusEl.className  = 'resp-status pending';
    statusEl.innerHTML  = '<span class="spinner"></span>Procesando solicitud RPC…';
    timeEl.textContent  = '';
    bodyEl.textContent  = '';

    const t0 = performance.now();
    try {
      const resp = await fetch(`/rpc_call/${encodeURIComponent(payload)}`);
      const elapsed = ((performance.now() - t0) / 1000).toFixed(2);
      const text = await resp.text();

      if (resp.ok) {
        statusEl.className = 'resp-status ok';
        statusEl.textContent = `HTTP ${resp.status} — OK`;
      } else {
        statusEl.className = 'resp-status error';
        statusEl.textContent = `HTTP ${resp.status} — Error`;
      }

      timeEl.textContent = `Tiempo de respuesta: ${elapsed} s`;
      bodyEl.textContent = text;

    } catch (err) {
      const elapsed = ((performance.now() - t0) / 1000).toFixed(2);
      statusEl.className = 'resp-status error';
      statusEl.textContent = 'Error de red';
      timeEl.textContent = `Tiempo transcurrido: ${elapsed} s`;
      bodyEl.textContent = err.toString();
    } finally {
      btn.disabled = false;
    }
  }
</script>
</body>
</html>"""
    return html


@app.route('/rpc_call/<payload>')
def rpc_call(payload):
    """Envía un mensaje al Worker vía RabbitMQ y espera la respuesta."""
    print(f"[Flask-RPC] Nueva solicitud: {payload}")

    try:
        client = get_rpc_client()
    except Exception as e:
        print(f"[Flask-RPC] Error al conectar RPC Client: {e}")
        return _render_respuesta(payload, None, error=str(e)), 503

    corr_id = client.send_request(payload)

    intentos = 0
    MAX_INTENTOS = 300
    while True:
        with client._lock:
            valor = client.queue.get(corr_id)
        if valor is not None:
            break
        if intentos >= MAX_INTENTOS:
            print(f"[Flask-RPC] TIMEOUT para corr_id={corr_id}")
            with client._lock:
                client.queue.pop(corr_id, None)
            return _render_respuesta(payload, None, error="Timeout: el Worker no respondió en 30 s."), 504
        sleep(0.1)
        intentos += 1

    with client._lock:
        respuesta = client.queue.pop(corr_id)

    print(f"[Flask-RPC] Respuesta recibida: {respuesta}")
    return _render_respuesta(payload, respuesta)


def _render_respuesta(payload, respuesta_raw, error=None):
    """Genera una página HTML estilizada con el resultado de la llamada RPC."""
    from datetime import datetime, timezone
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    if error:
        estado_badge = '<span class="badge badge-error">● ERROR</span>'
        cuerpo_html  = f'<p class="field-val error-text">{error}</p>'
    else:
        # Parsear campos del texto devuelto por el worker
        lines = respuesta_raw if isinstance(respuesta_raw, str) else respuesta_raw.decode('utf-8')
        estado_badge = '<span class="badge badge-ok">● OK</span>'

        def extraer(label, texto):
            for line in texto.splitlines():
                if label in line:
                    return line.split(':', 1)[-1].strip()
            return '—'

        resultado   = extraer('Resultado proceso', lines)
        nodo        = extraer('Nodo Worker',       lines)
        cola        = extraer('Cola origen',       lines)
        ts_worker   = extraer('Timestamp',         lines)
        descripcion = extraer('Descripción',       lines)

        cuerpo_html = f"""
        <div class="fields">
          <div class="field-row">
            <span class="field-key">Mensaje enviado</span>
            <span class="field-val">{payload}</span>
          </div>
          <div class="field-row highlight">
            <span class="field-key">Resultado procesado</span>
            <span class="field-val accent">{resultado}</span>
          </div>
          <div class="field-row">
            <span class="field-key">Nodo Worker</span>
            <span class="field-val">{nodo}</span>
          </div>
          <div class="field-row">
            <span class="field-key">Cola AMQP</span>
            <span class="field-val">{cola}</span>
          </div>
          <div class="field-row">
            <span class="field-key">Timestamp Worker</span>
            <span class="field-val">{ts_worker}</span>
          </div>
          <div class="field-row">
            <span class="field-key">Timestamp Cliente</span>
            <span class="field-val">{timestamp}</span>
          </div>
        </div>
        <div class="desc-box">
          <p class="desc-label">DESCRIPCIÓN DEL PROCESO</p>
          <p class="desc-text">El cliente Flask publicó el payload <code>{payload}</code> en la cola
          <code>rpc_queue</code> de CloudAMQP. El Worker consumió el mensaje, aplicó la transformación
          definida y retornó la respuesta mediante la cola de retorno exclusiva del cliente
          (<code>reply_to</code>), usando el <code>correlation_id</code> para correlacionar la respuesta.</p>
        </div>
        """

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Respuesta RPC — {payload}</title>
  <link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=Libre+Baskerville:ital,wght@0,400;0,700;1,400&display=swap" rel="stylesheet"/>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
    :root {{
      --bg:      #0d0f14;
      --surface: #13161e;
      --border:  #1e2330;
      --accent:  #4f8ef7;
      --accent2: #7dd3a8;
      --danger:  #e06c75;
      --text:    #cdd6f4;
      --muted:   #6c7086;
      --mono:    'IBM Plex Mono', monospace;
      --serif:   'Libre Baskerville', Georgia, serif;
    }}
    body {{
      background: var(--bg); color: var(--text);
      font-family: var(--serif);
      min-height: 100vh;
      display: flex; flex-direction: column; align-items: center;
      padding: 3rem 1.5rem 5rem;
    }}
    body::before {{
      content: ''; position: fixed; inset: 0;
      background-image:
        linear-gradient(rgba(79,142,247,.04) 1px, transparent 1px),
        linear-gradient(90deg, rgba(79,142,247,.04) 1px, transparent 1px);
      background-size: 40px 40px; pointer-events: none;
    }}
    header {{
      width: 100%; max-width: 760px;
      border-bottom: 1px solid var(--border);
      padding-bottom: 1.4rem; margin-bottom: 2rem;
    }}
    .institution {{
      font-family: var(--mono); font-size: .7rem;
      letter-spacing: .18em; color: var(--muted);
      text-transform: uppercase; margin-bottom: .5rem;
    }}
    h1 {{ font-size: 1.5rem; font-weight: 700; color: #e6eaf7; }}
    h1 span {{ color: var(--accent); }}
    .sub {{ margin-top: .4rem; font-size: .85rem; color: var(--muted); font-style: italic; }}
    .badge-row {{ display: flex; gap: .6rem; margin-top: .9rem; flex-wrap: wrap; }}
    .badge {{
      font-family: var(--mono); font-size: .68rem; letter-spacing: .08em;
      padding: .22rem .65rem; border-radius: 3px; border: 1px solid;
    }}
    .badge-ok    {{ color: var(--accent2); border-color: var(--accent2); background: rgba(125,211,168,.07); }}
    .badge-error {{ color: var(--danger);  border-color: var(--danger);  background: rgba(224,108,117,.07); }}
    .badge-blue  {{ color: var(--accent);  border-color: var(--accent);  background: rgba(79,142,247,.07); }}
    .badge-muted {{ color: var(--muted);   border-color: var(--border);  background: transparent; }}
    main {{ width: 100%; max-width: 760px; }}
    .card {{
      background: var(--surface); border: 1px solid var(--border);
      border-radius: 6px; padding: 1.6rem 1.8rem; margin-bottom: 1.4rem;
    }}
    .card-title {{
      font-family: var(--mono); font-size: .68rem; letter-spacing: .16em;
      color: var(--muted); text-transform: uppercase; margin-bottom: 1.2rem;
      display: flex; align-items: center; gap: .5rem;
    }}
    .card-title::before {{
      content: ''; display: inline-block; width: 6px; height: 6px;
      border-radius: 50%; background: var(--accent);
    }}
    .fields {{ display: flex; flex-direction: column; gap: .1rem; }}
    .field-row {{
      display: flex; align-items: baseline;
      padding: .55rem .7rem; border-radius: 4px;
      gap: 1rem;
    }}
    .field-row:nth-child(odd) {{ background: rgba(255,255,255,.02); }}
    .field-row.highlight {{ background: rgba(79,142,247,.07); border: 1px solid rgba(79,142,247,.15); }}
    .field-key {{
      font-family: var(--mono); font-size: .75rem;
      color: var(--muted); min-width: 180px; flex-shrink: 0;
    }}
    .field-val {{
      font-family: var(--mono); font-size: .85rem; color: var(--text);
      word-break: break-all;
    }}
    .field-val.accent {{ color: var(--accent2); font-weight: 600; font-size: 1rem; }}
    .field-val.error-text {{ color: var(--danger); }}
    .desc-box {{
      margin-top: 1.3rem; background: var(--bg);
      border: 1px solid var(--border); border-radius: 4px;
      padding: 1rem 1.2rem;
    }}
    .desc-label {{
      font-family: var(--mono); font-size: .65rem; letter-spacing: .14em;
      color: var(--muted); text-transform: uppercase; margin-bottom: .6rem;
    }}
    .desc-text {{ font-size: .88rem; color: var(--muted); line-height: 1.7; }}
    .desc-text code {{
      font-family: var(--mono); font-size: .8rem;
      color: var(--accent); background: rgba(79,142,247,.08);
      padding: .1rem .35rem; border-radius: 3px;
    }}
    .back-link {{
      display: inline-flex; align-items: center; gap: .4rem;
      font-family: var(--mono); font-size: .78rem; color: var(--accent);
      text-decoration: none; margin-bottom: 1.5rem;
      opacity: .8; transition: opacity .2s;
    }}
    .back-link:hover {{ opacity: 1; }}
    footer {{
      margin-top: 3rem; font-family: var(--mono); font-size: .68rem;
      color: var(--muted); text-align: center; letter-spacing: .06em;
    }}
  </style>
</head>
<body>
<header>
  <p class="institution">Laboratorio de Sistemas Distribuidos &mdash; Arquitecturas de Mensajería</p>
  <h1>Respuesta <span>RPC</span></h1>
  <p class="sub">Resultado de la invocación remota sobre cola AMQP</p>
  <div class="badge-row">
    {estado_badge}
    <span class="badge badge-blue">CloudAMQP · AMQPS</span>
    <span class="badge badge-muted">rpc_queue</span>
  </div>
</header>

<main>
  <a class="back-link" href="/">← Volver al panel</a>
  <div class="card">
    <p class="card-title">Resultado de la llamada RPC</p>
    {cuerpo_html}
  </div>
</main>

<footer>
  SISTEMAS DISTRIBUIDOS &nbsp;·&nbsp; ARQUITECTURAS DE MENSAJERÍA &nbsp;·&nbsp; PATRÓN RPC SOBRE AMQP
</footer>
</body>
</html>"""
    return html


# ── Arranque ────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port, debug=False)