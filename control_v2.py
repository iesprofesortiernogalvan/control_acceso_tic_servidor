import csv
import time
import threading
import paho.mqtt.client as mqtt

"""
centro/tic1/nukihub/lock/state
centro/tic1/nukihub/lock/battery/level
centro/tic1/nukihub/lock/battery/critical
centro/tic1/nukihub/lock/action
"""

# =========================================================
# CONFIGURACIÓN
# =========================================================
BROKER = "broker.hivemq.com"
PUERTO = 1883
TIC_ID = "1"

TOPIC_BASE = f"centro/tic{TIC_ID}"

# ---------- Topics de la placa ----------
TOPIC_ENTRADA = f"{TOPIC_BASE}/entrada"
TOPIC_EVENTO = f"{TOPIC_BASE}/evento"
TOPIC_ESTADO_GET = f"{TOPIC_BASE}/estado/get"
TOPIC_ESTADO = f"{TOPIC_BASE}/estado"
TOPIC_MENSAJE = f"{TOPIC_BASE}/mensaje"

# ---------- Base NukiHub ----------
NUKI_BASE = f"{TOPIC_BASE}/nukihub"

# ---------- Action / estados ----------
NUKI_LOCK_ACTION = f"{NUKI_BASE}/lock/action"
NUKI_LOCK_STATE = f"{NUKI_BASE}/lock/state"
NUKI_LOCK_HASTATE = f"{NUKI_BASE}/lock/hastate"
NUKI_LOCK_COMMAND_RESULT = f"{NUKI_BASE}/lock/commandResult"
NUKI_LOCK_COMPLETION_STATUS = f"{NUKI_BASE}/lock/completionStatus"
NUKI_LOCK_DOOR_SENSOR_STATE = f"{NUKI_BASE}/lock/doorSensorState"

# ---------- Queries ----------
NUKI_LOCK_QUERY_LOCKSTATE = f"{NUKI_BASE}/lock/query/lockstate"
NUKI_LOCK_QUERY_BATTERY = f"{NUKI_BASE}/lock/query/battery"

# ---------- Battery ----------
NUKI_LOCK_BATTERY_LEVEL = f"{NUKI_BASE}/lock/battery/level"
NUKI_LOCK_BATTERY_CRITICAL = f"{NUKI_BASE}/lock/battery/critical"
NUKI_LOCK_BATTERY_CHARGING = f"{NUKI_BASE}/lock/battery/charging"
NUKI_LOCK_BATTERY_VOLTAGE = f"{NUKI_BASE}/lock/battery/voltage"

# ---------- Maintenance ----------
NUKI_MAINT_MQTT_STATE = f"{NUKI_BASE}/maintenance/mqttConnectionState"

# ---------- CSV ----------
FICHERO_PROFESORES = "profesores.csv"

# =========================================================
# ESTADO GLOBAL
# =========================================================
profesores = {}
state_lock = threading.Lock()

nuki = {
    "state": "desconocido",          # estado simplificado para la placa
    "raw_state": "",
    "raw_hastate": "",
    "command_result": "",
    "completion_status": "",
    "door_sensor_state": "",
    "hub_online": False,
    "battery_level": None,
    "battery_critical": None,
    "battery_charging": None,
    "battery_voltage": None,
    "action_ack": "",
}

accion_en_curso = None   # "apertura" / "cierre" / None


# =========================================================
# NORMALIZACIÓN UID
# Compatible con:
# - RC522 en HEX: 6903C148
# - RDM6300/llavero impreso en decimal: 0007897366
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
    """
    Devuelve varias representaciones equivalentes de la UID.
    Sirve para que el sistema acepte:
    - CSV en HEX
    - CSV en decimal
    - lector RC522
    - lector RDM6300
    """
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
# CSV
# Formato: id,nombre,ap1,ap2
# El campo 'id' puede estar en:
# - HEX  (ej: 6903C148)
# - DEC  (ej: 0007897366)
# Se registran variantes equivalentes para compatibilidad.
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

                # Guardamos todas las variantes equivalentes
                for clave in variantes_uid(codigo):
                    profesores[clave] = datos

        print(f"Profesores cargados: {len(profesores)} entradas indexadas")
        print("UIDs cargadas:")
        for k in profesores:
            print(repr(k))

    except FileNotFoundError:
        print(f"ERROR: No existe {FICHERO_PROFESORES}")
    except Exception as e:
        print(f"ERROR al cargar profesores: {e}")


# =========================================================
# HELPERS HACIA LA PLACA
# =========================================================
def publicar_estado_placa(cliente, estado):
    cliente.publish(TOPIC_ESTADO, estado)
    print(f"[PLACA][ESTADO] {TOPIC_ESTADO} -> {estado}")


def publicar_mensaje_placa(cliente, msg):
    cliente.publish(TOPIC_MENSAJE, msg)
    print(f"[PLACA][MENSAJE] {TOPIC_MENSAJE} -> {msg}")


def responder_estado_placa(cliente):
    with state_lock:
        estado = nuki["state"]
        battery = nuki["battery_level"]
        critical = nuki["battery_critical"]

    publicar_estado_placa(cliente, estado)

    if critical == "1":
        publicar_mensaje_placa(cliente, "bateria critica")
    elif battery is not None:
        publicar_mensaje_placa(cliente, f"bateria {battery}%")
    else:
        publicar_mensaje_placa(cliente, "bateria desconocida")


# =========================================================
# HELPERS HACIA NUKI
# =========================================================
def consultar_estado_nuki(cliente):
    cliente.publish(NUKI_LOCK_QUERY_LOCKSTATE, "1")
    print(f"[NUKI][QUERY] {NUKI_LOCK_QUERY_LOCKSTATE} -> 1")


def consultar_bateria_nuki(cliente):
    cliente.publish(NUKI_LOCK_QUERY_BATTERY, "1")
    print(f"[NUKI][QUERY] {NUKI_LOCK_QUERY_BATTERY} -> 1")


def enviar_accion_nuki(cliente, accion):
    cliente.publish(NUKI_LOCK_ACTION, accion)
    print(f"[NUKI][ACTION] {NUKI_LOCK_ACTION} -> {accion}")


# =========================================================
# MAPEO DE ESTADOS NUKI -> PLACA
# =========================================================
def mapear_estado_nuki(hastate, state):
    hastate = (hastate or "").strip().lower()
    state = (state or "").strip().lower()

    if hastate == "locking":
        return "cerrando"
    if hastate == "locked":
        return "cerrado"
    if hastate == "unlocking":
        return "abriendo"
    if hastate == "unlocked":
        return "abierto"
    if hastate == "jammed":
        return "error"

    if state == "locked":
        return "cerrado"
    if state in ("unlocked", "unlatched", "unlockedlnga"):
        return "abierto"
    if state == "unlatching":
        return "abriendo"
    if state == "motorblocked":
        return "error"
    if state == "uncalibrated":
        return "desconocido"

    return "desconocido"


def actualizar_estado_derivado(cliente):
    with state_lock:
        nuevo = mapear_estado_nuki(nuki["raw_hastate"], nuki["raw_state"])
        cambio = nuevo != nuki["state"]
        nuki["state"] = nuevo

    if cambio:
        publicar_estado_placa(cliente, nuevo)


# =========================================================
# LÓGICA DE TARJETA
# =========================================================
def procesar_tarjeta(cliente, uid):
    global accion_en_curso

    uid_raw = uid
    uid_n = normalizar_uid(uid)
    posibles = variantes_uid(uid_n)

    print(f"[UID RAW] {repr(uid_raw)}")
    print(f"[UID NORMALIZADA] {repr(uid_n)}")
    print(f"[UID VARIANTES] {posibles}")

    usuario = None
    uid_encontrada = None

    for clave in posibles:
        if clave in profesores:
            usuario = profesores[clave]
            uid_encontrada = clave
            break

    if usuario is None:
        print("[DEBUG] No encontrada en profesores")
        print("[DEBUG] Claves cargadas:")
        for k in profesores:
            print(f" - {repr(k)}")
        publicar_mensaje_placa(cliente, "usuario no autorizado")
        return

    nombre = usuario["nombre"]
    ap1 = usuario["apellido1"]
    ap2 = usuario["apellido2"]

    print(f"[OK] UID encontrada como {repr(uid_encontrada)}")
    print(f"[OK] Usuario autorizado: {nombre} {ap1} {ap2}")

    publicar_mensaje_placa(cliente, f"Bienvenido {nombre}")

    with state_lock:
        estado_actual = nuki["state"]

    if estado_actual == "cerrado":
        accion_en_curso = "apertura"
        publicar_mensaje_placa(cliente, "enviando apertura")
        enviar_accion_nuki(cliente, "unlock")

    elif estado_actual == "abierto":
        accion_en_curso = "cierre"
        publicar_mensaje_placa(cliente, "enviando cierre")
        enviar_accion_nuki(cliente, "lock")

    elif estado_actual in ("abriendo", "cerrando"):
        publicar_mensaje_placa(cliente, "cerradura ocupada")

    elif estado_actual == "error":
        publicar_mensaje_placa(cliente, "cerradura bloqueada")
        consultar_estado_nuki(cliente)

    else:
        publicar_mensaje_placa(cliente, "consultando estado")
        consultar_estado_nuki(cliente)


# =========================================================
# CALLBACKS MQTT
# =========================================================
def on_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        print("Conectado al broker MQTT")

        # Placa
        client.subscribe(TOPIC_ENTRADA)
        client.subscribe(TOPIC_EVENTO)
        client.subscribe(TOPIC_ESTADO_GET)

        # NukiHub
        client.subscribe(NUKI_LOCK_ACTION)               # para ver el ack
        client.subscribe(NUKI_LOCK_STATE)
        client.subscribe(NUKI_LOCK_HASTATE)
        client.subscribe(NUKI_LOCK_COMMAND_RESULT)
        client.subscribe(NUKI_LOCK_COMPLETION_STATUS)
        client.subscribe(NUKI_LOCK_DOOR_SENSOR_STATE)

        client.subscribe(NUKI_LOCK_BATTERY_LEVEL)
        client.subscribe(NUKI_LOCK_BATTERY_CRITICAL)
        client.subscribe(NUKI_LOCK_BATTERY_CHARGING)
        client.subscribe(NUKI_LOCK_BATTERY_VOLTAGE)

        client.subscribe(NUKI_MAINT_MQTT_STATE)

        print("Suscripciones activas")

        consultar_estado_nuki(client)
        consultar_bateria_nuki(client)
        responder_estado_placa(client)

    else:
        print(f"Error al conectar. Código: {reason_code}")


def on_message(client, userdata, msg):
    global accion_en_curso

    try:
        topic = msg.topic
        payload = msg.payload.decode("utf-8", errors="ignore").strip()

        print(f"[REC] {topic} -> {payload}")

        # =====================================================
        # Topics de la placa
        # =====================================================
        if topic == TOPIC_ENTRADA:
            procesar_tarjeta(client, payload)
            return

        if topic == TOPIC_EVENTO:
            print(f"[EVENTO PLACA] {payload}")
            return

        if topic == TOPIC_ESTADO_GET:
            responder_estado_placa(client)
            consultar_estado_nuki(client)
            consultar_bateria_nuki(client)
            return

        # =====================================================
        # Topics de NukiHub
        # =====================================================

        # Ack de acción
        if topic == NUKI_LOCK_ACTION:
            with state_lock:
                nuki["action_ack"] = payload

            if payload == "ack":
                if accion_en_curso == "apertura":
                    publicar_mensaje_placa(client, "apertura aceptada")
                elif accion_en_curso == "cierre":
                    publicar_mensaje_placa(client, "cierre aceptado")
            return

        if topic == NUKI_MAINT_MQTT_STATE:
            with state_lock:
                nuki["hub_online"] = (payload.lower() == "online")

            if payload.lower() == "online":
                print("[NUKI] NukiHub online")
                consultar_estado_nuki(client)
                consultar_bateria_nuki(client)
            else:
                print("[NUKI] NukiHub offline")
                publicar_mensaje_placa(client, "nukihub offline")
            return

        if topic == NUKI_LOCK_STATE:
            with state_lock:
                nuki["raw_state"] = payload
            actualizar_estado_derivado(client)
            return

        if topic == NUKI_LOCK_HASTATE:
            with state_lock:
                nuki["raw_hastate"] = payload
            actualizar_estado_derivado(client)

            estado = mapear_estado_nuki(payload, nuki["raw_state"])

            if estado == "abriendo":
                publicar_mensaje_placa(client, "abriendo")
            elif estado == "cerrando":
                publicar_mensaje_placa(client, "cerrando")
            elif estado == "abierto":
                publicar_mensaje_placa(client, "abierto")
                accion_en_curso = None
            elif estado == "cerrado":
                publicar_mensaje_placa(client, "cerrado")
                accion_en_curso = None
            elif estado == "error":
                publicar_mensaje_placa(client, "cerradura bloqueada")
                accion_en_curso = None
            return

        if topic == NUKI_LOCK_COMMAND_RESULT:
            with state_lock:
                nuki["command_result"] = payload

            print(f"[NUKI] commandResult={payload}")

            if payload == "failed":
                publicar_mensaje_placa(client, "fallo al enviar orden")
                accion_en_curso = None
            elif payload == "timeOut":
                publicar_mensaje_placa(client, "timeout cerradura")
                accion_en_curso = None
            elif payload == "notPaired":
                publicar_mensaje_placa(client, "nuki no emparejada")
                accion_en_curso = None
            elif payload == "working":
                publicar_mensaje_placa(client, "procesando orden")
            return

        if topic == NUKI_LOCK_COMPLETION_STATUS:
            with state_lock:
                nuki["completion_status"] = payload

            print(f"[NUKI] completionStatus={payload}")

            if payload == "motorBlocked":
                publicar_mensaje_placa(client, "motor bloqueado")
                accion_en_curso = None
            elif payload == "busy":
                publicar_mensaje_placa(client, "cerradura ocupada")
            elif payload == "canceled":
                publicar_mensaje_placa(client, "accion cancelada")
                accion_en_curso = None
            elif payload == "tooRecent":
                publicar_mensaje_placa(client, "accion demasiado reciente")
                accion_en_curso = None
            elif payload == "success":
                pass
            return

        if topic == NUKI_LOCK_DOOR_SENSOR_STATE:
            with state_lock:
                nuki["door_sensor_state"] = payload
            print(f"[NUKI] doorSensorState={payload}")
            return

        # =====================================================
        # Batería
        # =====================================================
        if topic == NUKI_LOCK_BATTERY_LEVEL:
            with state_lock:
                nuki["battery_level"] = payload

            print(f"[NUKI] Battery level={payload}%")
            publicar_mensaje_placa(client, f"bateria {payload}%")
            return

        if topic == NUKI_LOCK_BATTERY_CRITICAL:
            with state_lock:
                nuki["battery_critical"] = payload

            if payload == "1":
                publicar_mensaje_placa(client, "bateria critica")
            return

        if topic == NUKI_LOCK_BATTERY_CHARGING:
            with state_lock:
                nuki["battery_charging"] = payload
            return

        if topic == NUKI_LOCK_BATTERY_VOLTAGE:
            with state_lock:
                nuki["battery_voltage"] = payload
            print(f"[NUKI] Battery voltage={payload}V")
            return

    except Exception as e:
        print(f"Error procesando mensaje MQTT: {e}")


# =========================================================
# QUERIES PERIÓDICAS
# =========================================================
def thread_queries_periodicas(cliente):
    while True:
        try:
            if cliente.is_connected():
                consultar_estado_nuki(cliente)
                consultar_bateria_nuki(cliente)
            time.sleep(120)
        except Exception as e:
            print(f"[THREAD QUERY] Error: {e}")
            time.sleep(5)


# =========================================================
# MAIN
# =========================================================
def main():
    cargar_profesores()

    cliente = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"servidor_tic{TIC_ID}_nuki"
    )
    cliente.on_connect = on_connect
    cliente.on_message = on_message

    try:
        print(f"Conectando a {BROKER}:{PUERTO} ...")
        cliente.connect(BROKER, PUERTO, 60)

        cliente.loop_start()

        t = threading.Thread(target=thread_queries_periodicas, args=(cliente,), daemon=True)
        t.start()

        print("Servidor activo. Ctrl+C para salir.")
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nDesconectando...")
        cliente.loop_stop()
        cliente.disconnect()
        print("Servidor detenido.")

    except Exception as e:
        print(f"Error general: {e}")


if __name__ == "__main__":
    main()



