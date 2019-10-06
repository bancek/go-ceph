package radosstriper

// #cgo LDFLAGS: -lrados -lradosstriper
// #include <errno.h>
// #include <stdlib.h>
// #include <rados/librados.h>
// #include <radosstriper/libradosstriper.h>
import "C"
import (
	"time"
	"unsafe"

	"github.com/ceph/go-ceph/rados"
)

// RadosStriper represents a Rados striper instance.
type RadosStriper struct {
	radosStriper C.rados_striper_t
}

func GetRadosStriper(ioctx *rados.IOContext) (*RadosStriper, error) {
	radosStriper := &RadosStriper{}
	ret := C.rados_striper_create(C.rados_ioctx_t(ioctx.Pointer()), &radosStriper.radosStriper)
	if ret != 0 {
		return nil, rados.RadosError(int(ret))
	}
	return radosStriper, nil
}

// Write writes len(data) bytes to the object with key oid starting at byte
// offset offset. It returns an error, if any.
func (s *RadosStriper) Write(soid string, data []byte, offset uint64) error {
	c_soid := C.CString(soid)
	defer C.free(unsafe.Pointer(c_soid))

	dataPointer := unsafe.Pointer(nil)
	if len(data) > 0 {
		dataPointer = unsafe.Pointer(&data[0])
	}

	ret := C.rados_striper_write(s.radosStriper, c_soid,
		(*C.char)(dataPointer),
		(C.size_t)(len(data)),
		(C.uint64_t)(offset))

	return rados.GetRadosError(int(ret))
}

// WriteFull writes len(data) bytes to the object with key oid.
// The object is filled with the provided data. If the object exists,
// it is atomically truncated and then written. It returns an error, if any.
func (s *RadosStriper) WriteFull(soid string, data []byte) error {
	c_soid := C.CString(soid)
	defer C.free(unsafe.Pointer(c_soid))

	ret := C.rados_striper_write_full(s.radosStriper, c_soid,
		(*C.char)(unsafe.Pointer(&data[0])),
		(C.size_t)(len(data)))

	return rados.GetRadosError(int(ret))
}

// Append appends len(data) bytes to the object with key oid.
// The object is appended with the provided data. If the object exists,
// it is atomically appended to. It returns an error, if any.
func (s *RadosStriper) Append(soid string, data []byte) error {
	c_soid := C.CString(soid)
	defer C.free(unsafe.Pointer(c_soid))

	ret := C.rados_striper_append(s.radosStriper, c_soid,
		(*C.char)(unsafe.Pointer(&data[0])),
		(C.size_t)(len(data)))

	return rados.GetRadosError(int(ret))
}

// Read reads up to len(data) bytes from the object with key oid starting at byte
// offset offset. It returns the number of bytes read and an error, if any.
func (s *RadosStriper) Read(soid string, data []byte, offset uint64) (int, error) {
	c_soid := C.CString(soid)
	defer C.free(unsafe.Pointer(c_soid))

	var buf *C.char
	if len(data) > 0 {
		buf = (*C.char)(unsafe.Pointer(&data[0]))
	}

	ret := C.rados_striper_read(
		s.radosStriper,
		c_soid,
		buf,
		(C.size_t)(len(data)),
		(C.uint64_t)(offset))

	if ret >= 0 {
		return int(ret), nil
	} else {
		return 0, rados.GetRadosError(int(ret))
	}
}

// Delete deletes the object with key oid. It returns an error, if any.
func (s *RadosStriper) Delete(soid string) error {
	c_soid := C.CString(soid)
	defer C.free(unsafe.Pointer(c_soid))

	return rados.GetRadosError(int(C.rados_striper_remove(s.radosStriper, c_soid)))
}

// Truncate resizes the object with key oid to size size. If the operation
// enlarges the object, the new area is logically filled with zeroes. If the
// operation shrinks the object, the excess data is removed. It returns an
// error, if any.
func (s *RadosStriper) Truncate(soid string, size uint64) error {
	c_soid := C.CString(soid)
	defer C.free(unsafe.Pointer(c_soid))

	return rados.GetRadosError(int(C.rados_striper_trunc(s.radosStriper, c_soid, (C.uint64_t)(size))))
}

// Destroy informs librados that the I/O context is no longer in use.
// Resources associated with the context may not be freed immediately, and the
// context should not be used again after calling this method.
func (s *RadosStriper) Destroy() {
	C.rados_striper_destroy(s.radosStriper)
}

// Stat returns the size of the object and its last modification time
func (s *RadosStriper) Stat(object string) (stat rados.ObjectStat, err error) {
	var c_psize C.uint64_t
	var c_pmtime C.time_t
	c_object := C.CString(object)
	defer C.free(unsafe.Pointer(c_object))

	ret := C.rados_striper_stat(
		s.radosStriper,
		c_object,
		&c_psize,
		&c_pmtime)

	if ret < 0 {
		return rados.ObjectStat{}, rados.GetRadosError(int(ret))
	}

	return rados.ObjectStat{
		Size:    uint64(c_psize),
		ModTime: time.Unix(int64(c_pmtime), 0),
	}, nil
}
