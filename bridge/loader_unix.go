//go:build !windows
// +build !windows

package bridge

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"unsafe"

	"github.com/ebitengine/purego"
)

// Bridge Rust FFI 接口
type Bridge struct {
	lib               uintptr
	abiVersion        func() uint32
	engineVersion     func(*uintptr, *uintptr) int32
	capabilities      func(*uintptr, *uintptr) int32
	lastError         func(*uintptr, *uintptr) int32
	lastErrorFree     func(uintptr, uintptr)
	planCompile       func(*byte, uintptr, *uint64) int32
	planFree          func(uint64)
	planExecuteSimple func(uint64, *byte, uintptr, *uintptr, *uintptr) int32
	outputFree        func(uintptr, uintptr)
}

// LoadBridge 加载动态库
func LoadBridge(libPath string) (*Bridge, error) {
	if libPath == "" {
		// 优先级：环境变量 > 可执行文件目录
		libPath = os.Getenv("POLARS_BRIDGE_LIB")
		if libPath == "" {
			exePath, err := os.Executable()
			if err != nil {
				return nil, fmt.Errorf("failed to get executable path: %w", err)
			}
			exeDir := filepath.Dir(exePath)
			libPath = filepath.Join(exeDir, getLibName())
		}
	}

	if _, err := os.Stat(libPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("library not found: %s", libPath)
	}

	lib, err := purego.Dlopen(libPath, purego.RTLD_NOW|purego.RTLD_GLOBAL)
	if err != nil {
		return nil, fmt.Errorf("failed to load library %s: %w", libPath, err)
	}

	b := &Bridge{lib: lib}

	// 加载所有函数
	purego.RegisterLibFunc(&b.abiVersion, lib, "bridge_abi_version")
	purego.RegisterLibFunc(&b.engineVersion, lib, "bridge_engine_version")
	purego.RegisterLibFunc(&b.capabilities, lib, "bridge_capabilities")
	purego.RegisterLibFunc(&b.lastError, lib, "bridge_last_error")
	purego.RegisterLibFunc(&b.lastErrorFree, lib, "bridge_last_error_free")
	purego.RegisterLibFunc(&b.planCompile, lib, "bridge_plan_compile")
	purego.RegisterLibFunc(&b.planFree, lib, "bridge_plan_free")
	purego.RegisterLibFunc(&b.planExecuteSimple, lib, "bridge_plan_execute_simple")
	purego.RegisterLibFunc(&b.outputFree, lib, "bridge_output_free")

	// 验证 ABI 版本
	abiVer := b.AbiVersion()
	if abiVer != 1 {
		return nil, fmt.Errorf("ABI version mismatch: expected 1, got %d", abiVer)
	}

	return b, nil
}

func getLibName() string {
	switch runtime.GOOS {
	case "windows":
		return "polars_bridge.dll"
	case "darwin":
		return "libpolars_bridge.dylib"
	default:
		return "libpolars_bridge.so"
	}
}

// AbiVersion 获取 ABI 版本
func (b *Bridge) AbiVersion() uint32 {
	return b.abiVersion()
}

// EngineVersion 获取引擎版本
func (b *Bridge) EngineVersion() (string, error) {
	var ptr uintptr
	var length uintptr
	ret := b.engineVersion(&ptr, &length)
	if ret != 0 {
		return "", b.getLastError()
	}
	return ptrToString(ptr, int(length)), nil
}

// Capabilities 获取能力信息
func (b *Bridge) Capabilities() (string, error) {
	var ptr uintptr
	var length uintptr
	ret := b.capabilities(&ptr, &length)
	if ret != 0 {
		return "", b.getLastError()
	}
	return ptrToString(ptr, int(length)), nil
}

// CompilePlan 编译计划
func (b *Bridge) CompilePlan(planBytes []byte) (uint64, error) {
	var handle uint64
	ret := b.planCompile(&planBytes[0], uintptr(len(planBytes)), &handle)
	runtime.KeepAlive(planBytes)

	if ret != 0 {
		return 0, b.getLastError()
	}
	return handle, nil
}

// FreePlan 释放计划
func (b *Bridge) FreePlan(handle uint64) {
	b.planFree(handle)
}

// ExecuteSimple 简单执行（JSON in/out）
func (b *Bridge) ExecuteSimple(handle uint64, inputJSON string) (string, error) {
	inputBytes := []byte(inputJSON)
	var outputPtr uintptr
	var outputLen uintptr

	ret := b.planExecuteSimple(
		handle,
		&inputBytes[0],
		uintptr(len(inputBytes)),
		&outputPtr,
		&outputLen,
	)
	runtime.KeepAlive(inputBytes)

	if ret != 0 {
		return "", b.getLastError()
	}

	output := ptrToString(outputPtr, int(outputLen))
	b.outputFree(outputPtr, outputLen)

	return output, nil
}

func (b *Bridge) getLastError() error {
	var ptr uintptr
	var length uintptr
	b.lastError(&ptr, &length)

	if ptr == 0 {
		return fmt.Errorf("unknown error")
	}

	errMsg := ptrToString(ptr, int(length))
	return fmt.Errorf("%s", errMsg)
}

func ptrToString(ptr uintptr, length int) string {
	if ptr == 0 || length == 0 {
		return ""
	}
	return unsafe.String((*byte)(unsafe.Pointer(ptr)), length)
}
