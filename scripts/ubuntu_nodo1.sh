#!/usr/bin/env bash
set -euo pipefail

usage() {
    echo "Uso: bash scripts/ubuntu_nodo1.sh --algo raft|2pc [--coordinator ID] --peer ID=HOST:PUERTO [--peer ID=HOST:PUERTO] [--id ID] [--members IDS] [--inspect] [--chat-feed] [--host HOST] [--port PUERTO] [--data-dir RUTA]. Para diagnóstico: --diagnostic"
    echo "Atajos compatibles: --n2 HOST:PUERTO --n3 HOST:PUERTO"
}

fail() {
    echo "ERROR: $*" >&2
    usage >&2
    exit 2
}

valid_port() {
    [[ $1 =~ ^[0-9]{1,5}$ ]] && (( 10#$1 >= 1 && 10#$1 <= 65535 ))
}

valid_endpoint() {
    [[ $1 =~ ^([a-zA-Z0-9_.-]+):([0-9]{1,5})$ ]] && valid_port "${BASH_REMATCH[2]}"
}

n2='' n3='' id=n1 host='0.0.0.0' port=9101 data_dir='data/n1' algo=raft coordinator='' members='' mode=serve chat_feed=false
peers=()
while (( $# )); do
    case "$1" in
        -h|--help) usage; exit 0 ;;
        --chat-feed) chat_feed=true; shift ;;
        --inspect|--diagnostic)
            [[ $mode == serve ]] || fail "Selecciona un solo modo"
            mode=${1#--}
            shift ;;
        --n2|--n3|--host|--port|--data-dir|--id|--algo|--members|--peer|--coordinator)
            (( $# >= 2 )) && [[ $2 != --* ]] || fail "Falta el valor de $1"
            case "$1" in
                --n2) n2=$2 ;;
                --n3) n3=$2 ;;
                --host) host=$2 ;;
                --port) port=$2 ;;
                --data-dir) data_dir=$2 ;;
                --id) id=$2 ;;
                --algo) algo=$2 ;;
                --coordinator) coordinator=$2 ;;
                --members) members=$2 ;;
                --peer) peers+=("$2") ;;
            esac
            shift 2 ;;
        *) fail "Argumento desconocido: $1" ;;
    esac
done

[[ $algo == raft || $algo == 2pc ]] || fail "--algo admite raft o 2pc"
if [[ $algo == 2pc ]]; then
    [[ $coordinator =~ ^[a-zA-Z0-9_-]{1,32}$ && $mode != diagnostic ]] || fail "2PC requiere --coordinator"
    [[ $data_dir != data/n1 ]] || fail "Indica un --data-dir separado para 2PC"
elif [[ -n $coordinator ]]; then
    fail "--coordinator requiere 2PC"
fi
[[ $id =~ ^[a-zA-Z0-9_-]{1,32}$ ]] || fail "ID inválido"
[[ $host =~ ^[a-zA-Z0-9_.-]+$ ]] || fail "--host requiere un nombre o IPv4"
valid_port "$port" || fail "--port debe estar entre 1 y 65535"
[[ $data_dir =~ [^[:space:]] ]] || fail "--data-dir requiere una ruta"

args=(--id "$id" --host "$host" --port "$port")
if [[ $chat_feed == true ]]; then
    [[ $mode == serve && $host == 127.0.0.1 ]] || fail "--chat-feed requiere servidor en 127.0.0.1"
    args+=(--chat-feed)
fi
if [[ $mode == diagnostic ]]; then
    [[ -z $n2 && -z $n3 && -z $members && ${#peers[@]} == 0 ]] || fail "--diagnostic no admite peers"
else
    [[ -z $n2 ]] || peers+=("n2=$n2")
    [[ -z $n3 ]] || peers+=("n3=$n3")
    (( ${#peers[@]} > 0 )) || fail "Configura los peers de Raft"
    args+=(--algo "$algo" --data-dir "$data_dir")
    [[ -z $coordinator ]] || args+=(--coordinator "$coordinator")
    for peer in "${peers[@]}"; do
        [[ $peer =~ ^[a-zA-Z0-9_-]{1,32}= ]] && valid_endpoint "${peer#*=}" || fail "Peer inválido: $peer"
        args+=(--peer "$peer")
    done
    [[ -z $members ]] || args+=(--members "$members")
    [[ $mode != inspect ]] || args+=(--inspect)
fi

cd "$(dirname "$0")/.."
python3 tools/build_all.py --java-only
exec java -cp build/java dogmessenger.NodeMain "${args[@]}"
