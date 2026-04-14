import csv
import time
import threading
import os
from datetime import datetime
import paho.mqtt.client as mqtt

# =========================================================
# CONFIGURACIÓN
# =========================================================

BROKER_ONLINE = "broker.hivemq.com"
BROKER_LOCAL = "127.0.0.1"

BROKER = BROKER_LOCAL
PUERTO = 1883
TIC_ID = "1"

TOPIC_BASE = f"centro/tic{TIC_ID}"

# -----------------------------
# Topics de la placa
# -----------------------------
TOPIC_ENTRADA = f"{TOPIC_BASE}/entrada"
TOPIC_EVENTO = f"{TOPIC_BASE}/evento"
TOPIC_ESTADO_GET = f"{TOPIC_BASE}/estado/get"
TOPIC_ESTADO = f"{TOPIC_BASE}/estado"
TOPIC_MENSAJE = f"{TOPIC_BASE}/mensaje"

# -----------------------------
# Topics mínimos de Nuki Hub
# -----------------------------
NUKI_BASE = f"{TOPIC_BASE}/nukihub"

NUKI_LOCK_STATE = f"{NUKI_BASE}/lock/state"
NUKI_LOCK_ACTION = f"{NUKI_BASE}/lock/action"
NUKI_LOCK_BATTERY_LEVEL = f"{NUKI_BASE}/lock/battery/level"
NUKI_LOCK_BATTERY_CRITICAL = f"{NUKI_BASE}/lock/battery/critical"
NUKI_LOCK_BATTERY_VOLTAGE = f"{NUKI_BASE}/lock/battery/voltage"  # opcional

# -----------------------------
# CSV de usuarios válidos
# Formato: id,nombre,ap1,ap2
# -----------------------------
FICHERO_PROFESORES = "profesores.csv"


# =========================================================
# LOG EN ARCHIVO CSV DIARIO
# =========================================================

def registrar_acceso_csv(usuario, accion):
    """ Genera un log diario en formato CSV con la info del usuario """
    ahora = datetime.now()
    # Nombre del archivo basado en la fecha: log_2024-05-20.csv
    nombre_fichero = ahora.strftime("log_%Y-%m-%d.csv")
    timestamp = ahora.strftime("%Y-%m-%d %H:%M:%S")

    # Extraemos los datos del diccionario 'usuario'
    id_prof = usuario.get("id_original", "S/N")
    nom = usuario.get("nombre", "Desconocido")
    ap1 = usuario.get("apellido1", "")
    ap2 = usuario.get("apellido2", "")
    aula = f"TIC{TIC_ID}"

    # Traducimos la acción de Nuki a algo legible
    accion_str = "ABRIR" if accion == "unlock" else "CERRAR"

    linea = [timestamp, id_prof, nom, ap1, ap2, aula, accion_str]

    try:
        # Modo 'a' (append) para no borrar lo anterior
        file_exists = os.path.isfile(nombre_fichero)
        with open(nombre_fichero, mode="a", encoding="utf-8-sig", newline="") as f:
            escritor = csv.writer(f)
            # Si el archivo es nuevo hoy, ponemos cabeceras
            if not file_exists:
                escritor.writerow(["TIMESTAMP", "ID_PROF", "NOM", "AP1", "AP2", "AULA", "ACCION"])
            escritor.writerow(linea)
    except Exception as e:
        log_error(f"Error escribiendo log diario: {e}")

# -----------------------------
# Timeout de seguridad
# -----------------------------
ACCION_TIMEOUT_S = 15

# -----------------------------
# Antirrebote de estados
# Ignora un cambio si llega exactamente igual que uno
# muy reciente ya notificado.
# -----------------------------
ESTADO_DEBOUNCE_S = 0.8

# -----------------------------
# Debug
# -----------------------------
DEBUG_MQTT_RX = False
DEBUG_BATTERY = False
USE_COLORS = True


# =========================================================
# LOGGING COLOREADO
# =========================================================

class LogColor:
    RESET = "\033[0m"
    GRAY = "\033[90m"
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    MAGENTA = "\033[95m"
    CYAN = "\033[96m"
    WHITE = "\033[97m"


def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


def _colorize(texto: str, color: str) -> str:
    if not USE_COLORS:
        return texto
    return f"{color}{texto}{LogColor.RESET}"


def log_raw(etiqueta: str, mensaje: str, color: str = LogColor.WHITE):
    timestamp = _colorize(f"[{_ts()}]", LogColor.GRAY)
    tag = _colorize(f"[{etiqueta}]", color)
    print(f"{timestamp} {tag} {mensaje}")


def log_main(msg: str):
    log_raw("MAIN", msg, LogColor.CYAN)


def log_mqtt(msg: str):
    log_raw("MQTT", msg, LogColor.BLUE)


def log_estado(msg: str):
    log_raw("ESTADO", msg, LogColor.GREEN)


def log_mensaje(msg: str):
    log_raw("MENSAJE", msg, LogColor.YELLOW)


def log_auth(msg: str):
    log_raw("AUTH", msg, LogColor.MAGENTA)


def log_nuki(msg: str):
    log_raw("NUKI", msg, LogColor.WHITE)


def log_warn(msg: str):
    log_raw("WARN", msg, LogColor.YELLOW)


def log_error(msg: str):
    log_raw("ERROR", msg, LogColor.RED)


# =========================================================
# ESTADO GLOBAL
# =========================================================

profesores = {}
state_lock = threading.Lock()

nuki = {
    "lock_state": "desconocido",   # locked / unlocked / locking / unlocking / desconocido
    "battery_level": None,
    "battery_critical": None,
    "battery_voltage": None,
}

accion_en_curso = None      # None / "lock" / "unlock"
accion_timestamp = 0.0

ultimo_mensaje_placa = None
ultimo_estado_notificado = None
ultimo_estado_notificado_ts = 0.0


GUARDA_ESTADOS_ANTIGUOS_S = 3.0

def estado_es_aceptable(nuevo_estado: str) -> bool:
    global accion_en_curso, accion_timestamp

    if accion_en_curso is None:
        return True

    edad = time.time() - accion_timestamp

    # Solo filtramos fuerte justo después de lanzar la orden
    if edad > GUARDA_ESTADOS_ANTIGUOS_S:
        return True

    if accion_en_curso == "unlock":
        # Tras pedir unlock, ignoramos locked viejo
        if nuevo_estado == "locked":
            return False

    elif accion_en_curso == "lock":
        # Tras pedir lock, ignoramos unlocked viejo
        if nuevo_estado == "unlocked":
            return False

    return True

# =========================================================
# NORMALIZACIÓN UID
# Compatible con:
# - RC522 en HEX: 6903C148
# - RDM6300 en decimal: 0007897366
# =========================================================

def normalizar_uid(uid: str) -> str:
    return (
        str(uid).strip()
        .upper()
        .replace(" ", "")
        .replace(":", "")
        .replace("-", "")
        .replace('"', "")
    )


def es_hex_valido(uid: str) -> bool:
    try:
        int(uid, 16)
        return True
    except Exception:
        return False


def es_decimal_valido(uid: str) -> bool:
    return uid.isdigit()


def uid_hex_a_decimal(uid_hex: str) -> str:
    valor = int(uid_hex, 16)
    return str(valor).zfill(10)


def uid_decimal_a_hex(uid_dec: str) -> str:
    valor = int(uid_dec, 10)
    return f"{valor:08X}"


def variantes_uid(uid: str) -> list[str]:
    uid_n = normalizar_uid(uid)
    variantes = {uid_n}

    if es_hex_valido(uid_n):
        try:
            variantes.add(uid_hex_a_decimal(uid_n))
        except Exception:
            pass

    if es_decimal_valido(uid_n):
        try:
            variantes.add(uid_decimal_a_hex(uid_n))
        except Exception:
            pass

    return list(variantes)


# =========================================================
# CARGA DE PROFESORES
# =========================================================

def cargar_profesores():
    global profesores
    profesores = {}

    try:
        with open(FICHERO_PROFESORES, mode="r", encoding="utf-8-sig") as f:
            lector = csv.reader(f, delimiter=",")

            for fila in lector:
                if len(fila) != 4:
                    continue

                codigo, nombre, ap1, ap2 = fila
                codigo = normalizar_uid(codigo)

                if codigo.lower() in ("id", "uid", "codigo", "codigotarjeta"):
                    continue

                datos = {
                    "nombre": nombre.strip(),
                    "apellido1": ap1.strip(),
                    "apellido2": ap2.strip(),
                    "id_original": codigo,
                }

                for clave in variantes_uid(codigo):
                    profesores[clave] = datos

        log_main(f"Profesores cargados: {len(profesores)} entradas indexadas")

    except FileNotFoundError:
        log_error(f"No existe {FICHERO_PROFESORES}")
    except Exception as e:
        log_error(f"Error al cargar profesores: {e}")


# =========================================================
# HELPERS HACIA LA PLACA
# =========================================================

def publicar_estado_placa(cliente, estado: str):
    global ultimo_estado_notificado, ultimo_estado_notificado_ts

    ahora = time.time()

    # Evita reenviar el mismo estado si se repite inmediatamente
    if ultimo_estado_notificado == estado and (ahora - ultimo_estado_notificado_ts) < ESTADO_DEBOUNCE_S:
        return

    ultimo_estado_notificado = estado
    ultimo_estado_notificado_ts = ahora

    cliente.publish(TOPIC_ESTADO, estado, qos=0, retain=False)
    log_estado(f"{TOPIC_ESTADO} -> {estado}")


def publicar_mensaje_placa(cliente, mensaje: str):
    global ultimo_mensaje_placa

    if ultimo_mensaje_placa == mensaje:
        return

    ultimo_mensaje_placa = mensaje
    cliente.publish(TOPIC_MENSAJE, mensaje, qos=0, retain=False)
    log_mensaje(f"{TOPIC_MENSAJE} -> {mensaje}")


def limpiar_ultimo_mensaje_placa():
    global ultimo_mensaje_placa
    ultimo_mensaje_placa = None

def responder_estado_placa(cliente):
    with state_lock:
        estado = nuki["lock_state"]
        nivel = nuki["battery_level"]
        critica = nuki["battery_critical"]
        voltaje = nuki["battery_voltage"]

    publicar_estado_placa(cliente, estado)

    # Solo avisos útiles
    if critica == "1":
        limpiar_ultimo_mensaje_placa()
        publicar_mensaje_placa(cliente, "bateria critica")
    elif nivel is not None:
        pass
    elif voltaje is not None:
        pass


# =========================================================
# HELPERS HACIA NUKI HUB
# =========================================================

def enviar_accion_nuki(cliente, accion: str):
    cliente.publish(NUKI_LOCK_ACTION, accion, qos=1, retain=False)
    log_nuki(f"{NUKI_LOCK_ACTION} -> {accion}")


# =========================================================
# CONTROL DE OPERACIÓN EN CURSO
# =========================================================

def iniciar_accion(accion: str):
    global accion_en_curso, accion_timestamp
    accion_en_curso = accion
    accion_timestamp = time.time()


def finalizar_accion():
    global accion_en_curso, accion_timestamp
    accion_en_curso = None
    accion_timestamp = 0.0


def accion_expirada() -> bool:
    if accion_en_curso is None:
        return False
    return (time.time() - accion_timestamp) > ACCION_TIMEOUT_S


def revisar_timeout_accion(cliente):
    if accion_expirada():
        log_warn("Operación expirada")
        limpiar_ultimo_mensaje_placa()
        publicar_mensaje_placa(cliente, "timeout operacion")
        finalizar_accion()


# =========================================================
# BÚSQUEDA DE USUARIO
# =========================================================

def buscar_usuario_por_uid(uid: str):
    uid_n = normalizar_uid(uid)
    posibles = variantes_uid(uid_n)

    if DEBUG_MQTT_RX:
        log_auth(f"UID raw={repr(uid)} normalizada={repr(uid_n)} variantes={posibles}")

    for clave in posibles:
        if clave in profesores:
            return profesores[clave], clave

    return None, None


# =========================================================
# DECISIÓN DE ACCIÓN
# =========================================================

def decidir_accion_segun_estado():
    with state_lock:
        estado = nuki["lock_state"]

    if estado == "locked":
        return "unlock", "enviando apertura"

    if estado == "unlocked":
        return "lock", "enviando cierre"

    if estado == "locking":
        return None, "cerrando"

    if estado == "unlocking":
        return None, "abriendo"

    return None, "estado desconocido"


# =========================================================
# PROCESAMIENTO DE TARJETA
# =========================================================
def procesar_tarjeta(cliente, uid: str):
    # Buscamos al usuario (la función busca tanto en hex como en dec)
    usuario, _ = buscar_usuario_por_uid(uid)

    if usuario is None:
        log_auth(f"UID {uid} no encontrada en la base de datos.")
        limpiar_ultimo_mensaje_placa()
        publicar_mensaje_placa(cliente, "usuario no autorizado")
        return

    # Si llegamos aquí, el usuario existe
    nombre_completo = f"{usuario['nombre']} {usuario['apellido1']}"
    log_auth(f"Acceso concedido a: {nombre_completo} (ID: {usuario['id_original']})")

    # Verificamos si hay una operación bloqueante en curso
    revisar_timeout_accion(cliente)
    if accion_en_curso is not None:
        publicar_mensaje_placa(cliente, "operacion en curso")
        return

    # Decidimos si toca abrir o cerrar según el estado actual del Nuki
    accion, mensaje_consola = decidir_accion_segun_estado()

    if accion is None:
        publicar_mensaje_placa(cliente, mensaje_consola)
        return

    # --- REGISTRO EN EL LOG DIARIO ---
    registrar_acceso_csv(usuario, accion)
    # ---------------------------------

    # Informamos a la placa y ejecutamos
    publicar_mensaje_placa(cliente, f"Hola {usuario['nombre']}")
    iniciar_accion(accion)
    enviar_accion_nuki(cliente, accion)

# =========================================================
# ACTUALIZACIÓN DE ESTADO NUKI
# =========================================================
def actualizar_estado_nuki(cliente, nuevo_estado: str):
    nuevo_estado = (nuevo_estado or "").strip().lower()

    if nuevo_estado not in ("locked", "unlocked", "locking", "unlocking"):
        return

    if not estado_es_aceptable(nuevo_estado):
        log_warn(f"Estado ignorado por tardío/incompatible: {nuevo_estado}")
        return

    with state_lock:
        anterior = nuki["lock_state"]

        if anterior == nuevo_estado:
            return

        nuki["lock_state"] = nuevo_estado

    publicar_estado_placa(cliente, nuevo_estado)

    if nuevo_estado == "locked" and accion_en_curso == "lock":
        finalizar_accion()

    elif nuevo_estado == "unlocked" and accion_en_curso == "unlock":
        finalizar_accion()

    log_nuki(f"Estado actualizado: {anterior} -> {nuevo_estado}")


# =========================================================
# CALLBACKS MQTT
# =========================================================

def on_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        log_mqtt("Conectado al broker")

        client.subscribe(TOPIC_ENTRADA)
        client.subscribe(TOPIC_EVENTO)
        client.subscribe(TOPIC_ESTADO_GET)

        client.subscribe(NUKI_LOCK_STATE)
        client.subscribe(NUKI_LOCK_BATTERY_LEVEL)
        client.subscribe(NUKI_LOCK_BATTERY_CRITICAL)
        client.subscribe(NUKI_LOCK_BATTERY_VOLTAGE)

        log_mqtt("Suscripciones activas")
        responder_estado_placa(client)
    else:
        log_error(f"Error al conectar. Código: {reason_code}")


def on_message(client, userdata, msg):
    try:
        topic = msg.topic
        payload = msg.payload.decode("utf-8", errors="ignore").strip()

        if DEBUG_MQTT_RX:
            log_mqtt(f"RX {topic} -> {payload}")

        if topic == TOPIC_ENTRADA:
            procesar_tarjeta(client, payload)
            return

        if topic == TOPIC_EVENTO:
            if DEBUG_MQTT_RX:
                log_mqtt(f"Evento placa: {payload}")
            return

        if topic == TOPIC_ESTADO_GET:
            responder_estado_placa(client)
            return

        if topic == NUKI_LOCK_STATE:
            actualizar_estado_nuki(client, payload)
            return

        if topic == NUKI_LOCK_BATTERY_LEVEL:
            with state_lock:
                anterior = nuki["battery_level"]
                if anterior == payload:
                    return
                nuki["battery_level"] = payload

            if DEBUG_BATTERY:
                log_nuki(f"Battery level = {payload}%")
            return

        if topic == NUKI_LOCK_BATTERY_CRITICAL:
            with state_lock:
                anterior = nuki["battery_critical"]
                if anterior == payload:
                    return
                nuki["battery_critical"] = payload

            if DEBUG_BATTERY:
                log_nuki(f"Battery critical = {payload}")

            if payload == "1":
                limpiar_ultimo_mensaje_placa()
                publicar_mensaje_placa(client, "bateria critica")
            return

        if topic == NUKI_LOCK_BATTERY_VOLTAGE:
            with state_lock:
                anterior = nuki["battery_voltage"]
                if anterior == payload:
                    return
                nuki["battery_voltage"] = payload

            if DEBUG_BATTERY:
                log_nuki(f"Battery voltage = {payload}V")
            return

    except Exception as e:
        log_error(f"MQTT: error procesando mensaje: {e}")


# =========================================================
# HILO DE VIGILANCIA
# =========================================================

def thread_vigilancia(cliente):
    while True:
        try:
            if cliente.is_connected():
                revisar_timeout_accion(cliente)
            time.sleep(1)
        except Exception as e:
            log_error(f"THREAD: {e}")
            time.sleep(2)


# =========================================================
# MAIN
# =========================================================

def main():
    cargar_profesores()

    cliente = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"servidor_tic{TIC_ID}_nuki_simple"
    )

    cliente.on_connect = on_connect
    cliente.on_message = on_message

    try:
        log_main(f"Conectando a {BROKER}:{PUERTO} ...")
        cliente.connect(BROKER, PUERTO, 60)

        cliente.loop_start()

        t = threading.Thread(target=thread_vigilancia, args=(cliente,), daemon=True)
        t.start()

        log_main("Servidor activo. Ctrl+C para salir.")

        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        log_warn("Desconectando...")
        cliente.loop_stop()
        cliente.disconnect()
        log_main("Servidor detenido.")

    except Exception as e:
        log_error(f"Error general: {e}")


if __name__ == "__main__":
    main()