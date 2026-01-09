package bridge

// Arrow C Data Interface structures
// https://arrow.apache.org/docs/format/CDataInterface.html

/*
#include <stdint.h>

// ArrowSchema describes the type and metadata of an Arrow array
struct ArrowSchema {
    const char* format;
    const char* name;
    const char* metadata;
    int64_t flags;
    int64_t n_children;
    struct ArrowSchema** children;
    struct ArrowSchema* dictionary;
    void (*release)(struct ArrowSchema*);
    void* private_data;
};

// ArrowArray contains the data buffers and child arrays
struct ArrowArray {
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void** buffers;
    struct ArrowArray** children;
    struct ArrowArray* dictionary;
    void (*release)(struct ArrowArray*);
    void* private_data;
};

// Helper functions to call release callbacks
void bridge_call_arrow_schema_release(struct ArrowSchema* schema) {
    if (schema->release) {
        schema->release(schema);
    }
}

void bridge_call_arrow_array_release(struct ArrowArray* array) {
    if (array->release) {
        array->release(array);
    }
}
*/
import "C"
import "unsafe"

// ArrowSchema represents Arrow schema in C
type ArrowSchema C.struct_ArrowSchema

// ArrowArray represents Arrow array data in C
type ArrowArray C.struct_ArrowArray

// ReleaseArrowSchema calls the release callback if set
func ReleaseArrowSchema(schema *ArrowSchema) {
	cSchema := (*C.struct_ArrowSchema)(unsafe.Pointer(schema))
	if cSchema.release != nil {
		C.bridge_call_arrow_schema_release(cSchema)
	}
}

// ReleaseArrowArray calls the release callback if set
func ReleaseArrowArray(array *ArrowArray) {
	cArray := (*C.struct_ArrowArray)(unsafe.Pointer(array))
	if cArray.release != nil {
		C.bridge_call_arrow_array_release(cArray)
	}
}
