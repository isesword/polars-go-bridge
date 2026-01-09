use std::cell::RefCell;
use std::ffi::CString;
use std::os::raw::{c_char, c_int};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::slice;
use prost::Message;

mod proto {
    include!("proto/polars_bridge.rs");
}

mod executor;
mod error;
mod arrow_bridge;

use error::{BridgeError, ErrorCode};

// ABI 版本
const ABI_VERSION: u32 = 1;

// 线程局部错误存储
thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = RefCell::new(None);
}

fn set_last_error(msg: String) {
    LAST_ERROR.with(|e| {
        *e.borrow_mut() = CString::new(msg).ok();
    });
}

fn clear_last_error() {
    LAST_ERROR.with(|e| {
        *e.borrow_mut() = None;
    });
}

// 错误处理宏
macro_rules! ffi_guard {
    ($body:expr) => {
        match catch_unwind(AssertUnwindSafe(|| $body)) {
            Ok(Ok(v)) => {
                clear_last_error();
                v
            }
            Ok(Err(e)) => {
                let (code, msg) = error::bridge_error_to_code(&e);
                set_last_error(format!("[{}] {}", code, msg));
                code as c_int
            }
            Err(panic) => {
                let msg = if let Some(s) = panic.downcast_ref::<&str>() {
                    s.to_string()
                } else if let Some(s) = panic.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "Unknown panic".to_string()
                };
                set_last_error(format!("[ERR_UNKNOWN] Panic: {}", msg));
                ErrorCode::Unknown as c_int
            }
        }
    };
}

// 1. 版本与能力
#[no_mangle]
pub extern "C" fn bridge_abi_version() -> u32 {
    ABI_VERSION
}

#[no_mangle]
pub extern "C" fn bridge_engine_version(ptr_out: *mut *const c_char, len_out: *mut usize) -> c_int {
    ffi_guard!({
        if ptr_out.is_null() || len_out.is_null() {
            return Err(BridgeError::InvalidArgument("Null output pointers".into()));
        }
        
        let version = CString::new(env!("CARGO_PKG_VERSION")).unwrap();
        unsafe {
            *ptr_out = version.as_ptr();
            *len_out = version.as_bytes().len();
        }
        std::mem::forget(version);
        Ok(0)
    })
}

#[no_mangle]
pub extern "C" fn bridge_capabilities(ptr_out: *mut *const c_char, len_out: *mut usize) -> c_int {
    ffi_guard!({
        if ptr_out.is_null() || len_out.is_null() {
            return Err(BridgeError::InvalidArgument("Null output pointers".into()));
        }
        
        let caps = r#"{
            "abi_version": 1,
            "min_plan_version_supported": 1,
            "max_plan_version_supported": 1,
            "supported_nodes": ["MemoryScan", "Project", "Filter", "WithColumns", "Limit"],
            "supported_exprs": ["Col", "Lit", "Binary", "Alias", "IsNull"],
            "supported_dtypes": ["Int64", "Float64", "Bool", "Utf8"],
            "execution_modes": ["collect"],
            "copy_behavior": "copy_on_boundary"
        }"#;
        
        let caps_cstr = CString::new(caps).unwrap();
        unsafe {
            *ptr_out = caps_cstr.as_ptr();
            *len_out = caps_cstr.as_bytes().len();
        }
        std::mem::forget(caps_cstr);
        Ok(0)
    })
}

// 2. 错误通道
#[no_mangle]
pub extern "C" fn bridge_last_error(ptr_out: *mut *const c_char, len_out: *mut usize) -> c_int {
    if ptr_out.is_null() || len_out.is_null() {
        return ErrorCode::InvalidArgument as c_int;
    }
    
    LAST_ERROR.with(|e| {
        if let Some(ref err) = *e.borrow() {
            unsafe {
                *ptr_out = err.as_ptr();
                *len_out = err.as_bytes().len();
            }
            0
        } else {
            unsafe {
                *ptr_out = ptr::null();
                *len_out = 0;
            }
            0
        }
    })
}

#[no_mangle]
pub extern "C" fn bridge_last_error_free(ptr: *const c_char, _len: usize) {
    if !ptr.is_null() {
        unsafe {
            let _ = CString::from_raw(ptr as *mut c_char);
        }
    }
}

// 3. Plan 编译
#[no_mangle]
pub extern "C" fn bridge_plan_compile(
    plan_bytes_ptr: *const u8,
    plan_bytes_len: usize,
    out_plan_handle_ptr: *mut u64,
) -> c_int {
    ffi_guard!({
        if plan_bytes_ptr.is_null() || out_plan_handle_ptr.is_null() {
            return Err(BridgeError::InvalidArgument("Null pointers".into()));
        }
        
        let plan_bytes = unsafe { slice::from_raw_parts(plan_bytes_ptr, plan_bytes_len) };
        let plan = proto::Plan::decode(plan_bytes)
            .map_err(|e| BridgeError::PlanDecode(e.to_string()))?;
        
        if plan.plan_version != 1 {
            return Err(BridgeError::PlanVersionUnsupported(plan.plan_version));
        }
        
        let handle = Box::into_raw(Box::new(plan)) as u64;
        unsafe {
            *out_plan_handle_ptr = handle;
        }
        
        Ok(0)
    })
}

#[no_mangle]
pub extern "C" fn bridge_plan_free(plan_handle: u64) {
    if plan_handle != 0 {
        unsafe {
            let _ = Box::from_raw(plan_handle as *mut proto::Plan);
        }
    }
}

// 4. 执行
#[no_mangle]
pub extern "C" fn bridge_plan_execute_simple(
    plan_handle: u64,
    input_json_ptr: *const c_char,
    input_json_len: usize,
    output_json_ptr: *mut *mut c_char,
    output_json_len: *mut usize,
) -> c_int {
    ffi_guard!({
        if plan_handle == 0 || input_json_ptr.is_null() || output_json_ptr.is_null() || output_json_len.is_null() {
            return Err(BridgeError::InvalidArgument("Null pointers".into()));
        }
        
        let plan = unsafe { &*(plan_handle as *const proto::Plan) };
        let input_json = unsafe { slice::from_raw_parts(input_json_ptr as *const u8, input_json_len) };
        let input_str = std::str::from_utf8(input_json)
            .map_err(|e| BridgeError::InvalidArgument(format!("Invalid UTF-8: {}", e)))?;
        
        let result_json = executor::execute_plan(plan, input_str)?;
        let result_cstr = CString::new(result_json).unwrap();
        
        unsafe {
            *output_json_len = result_cstr.as_bytes().len();
            *output_json_ptr = result_cstr.into_raw();
        }
        
        Ok(0)
    })
}

#[no_mangle]
pub extern "C" fn bridge_output_free(ptr: *mut c_char, _len: usize) {
    if !ptr.is_null() {
        unsafe {
            let _ = CString::from_raw(ptr);
        }
    }
}

// 5. Arrow-based execution (zero-copy)
use arrow::ffi::{FFI_ArrowSchema, FFI_ArrowArray};

#[no_mangle]
pub extern "C" fn bridge_plan_execute_arrow(
    plan_handle: u64,
    input_schema: *const FFI_ArrowSchema,
    input_array: *const FFI_ArrowArray,
    output_schema: *mut FFI_ArrowSchema,
    output_array: *mut FFI_ArrowArray,
) -> c_int {
    ffi_guard!({
        if plan_handle == 0 || input_schema.is_null() || input_array.is_null() 
            || output_schema.is_null() || output_array.is_null() {
            return Err(BridgeError::InvalidArgument("Null pointers".into()));
        }
        
        let plan = unsafe { &*(plan_handle as *const proto::Plan) };
        
        // 从 Arrow 导入 DataFrame
        let input_df = arrow_bridge::import_dataframe_from_arrow(input_schema, input_array)?;
        
        // 执行查询计划
        let root = plan.root.as_ref()
            .ok_or_else(|| BridgeError::PlanSemantic("Plan has no root node".into()))?;
        let result_df = executor::execute_node(root, input_df)?;
        
        // 导出为 Arrow
        arrow_bridge::export_dataframe_to_arrow(&result_df, output_schema, output_array)?;
        
        Ok(0)
    })
}
