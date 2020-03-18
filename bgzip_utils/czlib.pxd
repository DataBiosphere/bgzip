ctypedef unsigned char Bytef
ctypedef unsigned long uLongf
ctypedef unsigned int uInt
ctypedef z_stream_s z_stream

cdef extern from "zlib.h":
    extern struct z_stream_s:
        void * zalloc(void *, unsigned, unsigned) nogil
        void zfree(void *, void *) nogil
        uInt avail_in
        uInt avail_out
        const Bytef *next_in
        const Bytef *next_out
        uLongf total_out
        void * opaque

    extern int inflate(z_stream * strm, int flush) nogil
    extern int inflateInit2(z_stream * strm, int wbits) nogil
    extern int inflateEnd(z_stream *) nogil

    extern int deflate(z_stream * strm, int flush) nogil
    extern int deflateInit2(z_stream * strm, int level, int method, int wbits, int  mem_level, int strategy) nogil
    extern int deflateEnd(z_stream *) nogil

    extern uLongf crc32(uLongf crc, const Bytef * data, unsigned int len) nogil

    extern int Z_OK
    extern int Z_FINISH
    extern int Z_STREAM_END
    extern int Z_BEST_COMPRESSION
    extern int Z_DEFLATED
    extern int Z_DEFAULT_STRATEGY
