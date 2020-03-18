from cpython.object cimport PyObject, Py_buffer

cdef extern from "Python.h":
    extern PyObject * PyByteArray_FromStringAndSize(char *, int) nogil
    extern char * PyByteArray_AS_STRING(PyObject *) nogil
    extern int PyByteArray_Resize(PyObject *, int) nogil
    extern int  PyByteArray_Size(PyObject *) nogil

    extern PyObject * PyList_New(int size) nogil
    extern int PyList_SetItem(PyObject *, int, PyObject *) nogil
    extern PyObject * PyList_GetItem(PyObject *, int) nogil

    extern int PyBytes_GET_SIZE(PyObject *) nogil
    extern char * PyBytes_AS_STRING(PyObject *) nogil

    extern int PyMemoryView_Check(PyObject *) nogil
    extern char * PyMemoryView_GET_BUFFER(PyObject *) nogil
    extern PyObject * PyMemoryView_GET_BASE(PyObject *) nogil

    extern int PySequence_Size(PyObject *) nogil

    extern int PyObject_GetBuffer(PyObject *, Py_buffer *, int flags)
    extern void PyBuffer_Release(Py_buffer *)
    extern int PyBUF_SIMPLE

    extern void Py_INCREF(PyObject *) nogil
    extern void Py_DECREF(PyObject *) nogil
