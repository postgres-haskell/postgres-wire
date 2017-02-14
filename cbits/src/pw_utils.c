#include <pw_utils.h>

/* pointer to the next data message
   length of buffer
   ptr where result reasond would be put
   returns offset of buffer when datarows end.
*/
size_t scan_datarows(char *buffer, size_t len, int *reason)
{
    size_t offset = 0;
    uint32_t message_len = 0;

    while (1)
    {
        if (len - offset < HEADER_SIZE) {
            *reason = NEED_MORE_INPUT;
            break;
        }
        if (*(buffer + offset)!= DATAROW_HEADER) {
            *reason = OTHER_HEADER;
            break;
        }
        message_len = *(uint32_t*)(buffer + offset + HEADER_TYPE_SIZE);
        message_len = ntohl(message_len);
        if (len - offset - HEADER_TYPE_SIZE < (size_t)message_len) {
            *reason = NEED_MORE_INPUT;
            break;
        }
        offset = offset + HEADER_TYPE_SIZE + (size_t)message_len;
    }
    return offset;
}
