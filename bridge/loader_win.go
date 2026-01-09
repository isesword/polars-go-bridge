//go:build windows
// +build windows

package bridge

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"unsafe"
)

// Bridge Rust FFI 接口
type Bridge struct {
	lib               *syscall.DLL
	abiVersion        *syscall.Proc
	engineVersion     *syscall.Proc
	capabilities      *syscall.Proc
	lastError         *syscall.Proc
	lastErrorFree     *syscall.Proc
	planCompile       *syscall.Proc
	planFree          *syscall.Proc
	planExecuteSimple *syscall.Proc
	planExecuteArrow  *syscall.Proc
	outputFree        *syscall.Proc
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

	lib, err := syscall.LoadDLL(libPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load library %s: %w", libPath, err)
	}

	b := &Bridge{lib: lib}

	// 加载所有函数
	if b.abiVersion, err = lib.FindProc("bridge_abi_version"); err != nil {
		return nil, fmt.Errorf("failed to find bridge_abi_version: %w", err)
	}
	if b.engineVersion, err = lib.FindProc("bridge_engine_version"); err != nil {
		return nil, fmt.Errorf("failed to find bridge_engine_version: %w", err)
	}
	if b.capabilities, err = lib.FindProc("bridge_capabilities"); err != nil {
		return nil, fmt.Errorf("failed to find bridge_capabilities: %w", err)
	}
	if b.lastError, err = lib.FindProc("bridge_last_error"); err != nil {
		return nil, fmt.Errorf("failed to find bridge_last_error: %w", err)
	}
	if b.lastErrorFree, err = lib.FindProc("bridge_last_error_free"); err != nil {
		return nil, fmt.Errorf("failed to find bridge_last_error_free: %w", err)
	}
	if b.planCompile, err = lib.FindProc("bridge_plan_compile"); err != nil {
		return nil, fmt.Errorf("failed to find bridge_plan_compile: %w", err)
	}
	if b.planFree, err = lib.FindProc("bridge_plan_free"); err != nil {
		return nil, fmt.Errorf("failed to find bridge_plan_free: %w", err)
	}
	if b.planExecuteSimple, err = lib.FindProc("bridge_plan_execute_simple"); err != nil {
		return nil, fmt.Errorf("failed to find bridge_plan_execute_simple: %w", err)
	}
	if b.planExecuteArrow, err = lib.FindProc("bridge_plan_execute_arrow"); err != nil {
		return nil, fmt.Errorf("failed to find bridge_plan_execute_arrow: %w", err)
	}
	if b.outputFree, err = lib.FindProc("bridge_output_free"); err != nil {
		return nil, fmt.Errorf("failed to find bridge_output_free: %w", err)
	}

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
	ret, _, _ := b.abiVersion.Call()
	return uint32(ret)
}

// EngineVersion 获取引擎版本
func (b *Bridge) EngineVersion() (string, error) {
	var ptr uintptr
	var length uintptr
	ret, _, _ := b.engineVersion.Call(uintptr(unsafe.Pointer(&ptr)), uintptr(unsafe.Pointer(&length)))
	if ret != 0 {
		return "", b.getLastError()
	}
	return ptrToString(ptr, int(length)), nil
}

// Capabilities 获取能力信息
func (b *Bridge) Capabilities() (string, error) {
	var ptr uintptr
	var length uintptr
	ret, _, _ := b.capabilities.Call(uintptr(unsafe.Pointer(&ptr)), uintptr(unsafe.Pointer(&length)))
	if ret != 0 {
		return "", b.getLastError()
	}
	return ptrToString(ptr, int(length)), nil
}

// CompilePlan 编译计划
func (b *Bridge) CompilePlan(planBytes []byte) (uint64, error) {
	var handle uint64
	ret, _, _ := b.planCompile.Call(
		uintptr(unsafe.Pointer(&planBytes[0])),
		uintptr(len(planBytes)),
		uintptr(unsafe.Pointer(&handle)),
	)
	runtime.KeepAlive(planBytes)

	if ret != 0 {
		return 0, b.getLastError()
	}
	return handle, nil
}

// FreePlan 释放计划
func (b *Bridge) FreePlan(handle uint64) {
	b.planFree.Call(uintptr(handle))
}

// ExecuteSimple 简单执行（JSON in/out）
func (b *Bridge) ExecuteSimple(handle uint64, inputJSON string) (string, error) {
	inputBytes := []byte(inputJSON)
	var outputPtr uintptr
	var outputLen uintptr

	ret, _, _ := b.planExecuteSimple.Call(
		uintptr(handle),
		uintptr(unsafe.Pointer(&inputBytes[0])),
		uintptr(len(inputBytes)),
		uintptr(unsafe.Pointer(&outputPtr)),
		uintptr(unsafe.Pointer(&outputLen)),
	)
	runtime.KeepAlive(inputBytes)

	if ret != 0 {
		return "", b.getLastError()
	}

	output := ptrToString(outputPtr, int(outputLen))
	b.outputFree.Call(outputPtr, outputLen)

	return output, nil
}

func (b *Bridge) getLastError() error {
	var ptr uintptr
	var length uintptr
	b.lastError.Call(uintptr(unsafe.Pointer(&ptr)), uintptr(unsafe.Pointer(&length)))

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
	bytes := make([]byte, length)
	for i := 0; i < length; i++ {
		bytes[i] = *(*byte)(unsafe.Pointer(ptr + uintptr(i)))
	}
	return string(bytes)
}
