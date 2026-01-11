SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
export POLARS_BRIDGE_LIB="$SCRIPT_DIR/../polars_bridge.dll"

go test "$SCRIPT_DIR/../polars/" -v -count=1
