# 🖧 Sistema Distribuido RPC — Flask + RabbitMQ

<div align="center">

**UNIVERSIDAD NACIONAL DE SAN AGUSTÍN**  
**FACULTAD DE INGENIERÍA DE PRODUCCIÓN Y SERVICIOS**

| | |
|---|---|
| **Docente** | Jesús Martín Silva Fernández |
| **Curso** | Sistemas Distribuidos |
| **Escuela** | Ingeniería de Sistemas |
| **Fecha** | 04 - 05 - 2026 |

</div>

---

## 👥 Integrantes

| Apellidos y Nombres |
|---|
| Larico Rodriguez, Bryan Fernando |
| Maldonado Vilca, Victor Gonzalo |
| Quispe Huaman, Rodrigo Ferdinand |
| Salas Aguilar, Juan Victor |

---

## 📋 Descripción

Implementación del patrón **Remote Procedure Call (RPC)** usando **Flask** como cliente/servidor HTTP y **RabbitMQ** (CloudAMQP) como broker de mensajes. El sistema consta de dos componentes desplegados de forma independiente en **Render.com** que se comunican exclusivamente a través de colas de mensajes.

---

## 🏗️ Arquitectura

```
Usuario
  │
  ▼
[Flask Web Service]  ──publica──▶  [CloudAMQP / RabbitMQ]  ──consume──▶  [Worker]
  │                                      rpc_queue                           │
  └──────────────────recibe respuesta────────────────────────────────────────┘
```

| Componente | Archivo | Tipo en Render |
|---|---|---|
| Servidor Flask (Cliente RPC) | `amqpstorm_threaded_rpc_client.py` | Web Service |
| Worker RPC (Consumidor) | `worker.py` | Background Worker |
| Broker de mensajes | CloudAMQP (externo) | Servicio en la nube |

---

## 📁 Estructura del Repositorio

```
Sistemas_Distribuidos_RPC/
├── amqpstorm_threaded_rpc_client.py   # Servidor Flask + Cliente RPC asíncrono
├── worker.py             # Worker consumidor de mensajes RabbitMQ
├── requirements.txt      # Dependencias Python
└── README.md             # Este archivo
```

---

## ⚙️ Requisitos Previos

- Python 3.9 o superior
- Cuenta en [Render.com](https://render.com) (plan gratuito)
- Cuenta en [CloudAMQP](https://cloudamqp.com) (plan Little Lemur — gratis)
- Git

### Dependencias Python (`requirements.txt`)

```
flask
amqpstorm
gunicorn
```

---

## 🚀 Instalación y Ejecución Local

### 1. Clonar el repositorio

```bash
git clone https://github.com/Victor-Gonzalo-Maldonado-Vilca/Sistemas_Distribuidos_RPC.git
cd Sistemas_Distribuidos_RPC
```

### 2. Crear y activar entorno virtual

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Linux / Mac
source venv/bin/activate
```

### 3. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 4. Ejecutar en dos terminales

**Terminal 1 — Worker:**
```bash
python worker.py
```
Deberías ver:
```
 [*] Conectando a CloudAMQP...
 [*] CONECTADO. Esperando mensajes en 'rpc_queue'...
```

**Terminal 2 — Flask:**
```bash
python amqpstorm_threaded_rpc_client.py
```

Visita en el navegador:
```
http://localhost:5000/rpc_call/hola_mundo
```

Resultado esperado:
```
RESULTADO CLOUD: HOLA_MUNDO
```

---

## ☁️ Despliegue en Render.com

> ⚠️ **Importante:** Debes crear **dos servicios separados** en Render. Si solo tienes uno, el sistema no funcionará.

### Servicio 1 — Web Service (Flask)

| Campo | Valor |
|---|---|
| Name | sistemas-distribuidos-rpc |
| Environment | Python 3 |
| Build Command | `pip install -r requirements.txt` |
| Start Command | `gunicorn amqpstorm_threaded_rpc_client:app` |
| Plan | Free |

### Servicio 2 — Background Worker (Ideal)
Se uso Web Service, engañado a Render, debido a que Background Worker es de pago

| Campo | Valor |
|---|---|
| Name | sistemas-distribuidos-worker |
| Environment | Python 3 |
| Build Command | `pip install -r requirements.txt` |
| Start Command | `python worker.py` |
| Plan | Pay |

---

## 🔌 Configurar CloudAMQP

1. Crear cuenta en [cloudamqp.com](https://cloudamqp.com)
2. Crear nueva instancia → Plan **Little Lemur** (gratis)
3. Copiar la **AMQP URL** con formato:
   ```
   amqps://usuario:contraseña@servidor.cloudamqp.com/vhost
   ```
4. Reemplazar el valor de `URL_NUBE` en `amqpstorm_threaded_rpc_client.py` y `worker.py`

---

## 🌐 Endpoints Disponibles

| Ruta | Descripción |
|---|---|
| `GET /` | Página de estado del servidor |
| `GET /rpc_call/<mensaje>` | Envía `<mensaje>` al Worker y retorna la respuesta |

**Ejemplo:**
```
https://sistemas-distribuidos-rpc.onrender.com/rpc_call/prueba
→ RESULTADO CLOUD: PRUEBA
```

---

## 🔧 Solución de Errores Comunes

### ❌ `Error: El Worker no respondió a tiempo (Timeout)`
- Verificar que el **Background Worker** esté en estado `Live` en Render.
- En el plan gratuito, Render duerme los servicios tras 15 min de inactividad. El primer request tarda ~30s en despertar.
- Verificar que la `URL_NUBE` en ambos archivos sea correcta.

### ❌ Error 503 Service Unavailable
- El Web Service está arrancando (normal los primeros ~2 minutos tras el deploy).

### ❌ Error de conexión con RabbitMQ
- Verificar la URL de CloudAMQP desde el panel de la instancia.
- Asegurarse de que la instancia de CloudAMQP no haya expirado.

---

## 📌 Variables Clave

| Variable | Archivo | Descripción |
|---|---|---|
| `URL_NUBE` | Ambos | URL de conexión a CloudAMQP |
| `rpc_queue` | Ambos | Nombre de la cola de mensajes |
| `MAX_INTENTOS` | `amqpstorm_threaded_rpc_client.py` | Timeout: 300 × 0.1s = **30 segundos** |
| `PORT` | `worker.py` | Puerto del servidor de mantenimiento para Render |

---

## 📚 Flujo de Comunicación

1. El usuario accede a `/rpc_call/<mensaje>` en el Web Service de Flask.
2. Flask publica el mensaje en la cola `rpc_queue` de CloudAMQP.
3. El Worker consume el mensaje y ejecuta la lógica de negocio.
4. El Worker publica la respuesta en la cola de respuestas del cliente.
5. Flask recoge la respuesta y la retorna al usuario.

---

<div align="center">

**Universidad Nacional de San Agustín de Arequipa**  
Escuela Profesional de Ingeniería de Sistemas  
Curso: Sistemas Distribuidos — 2026

</div>
