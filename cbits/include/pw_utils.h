#include <stdint.h>
#include <stdlib.h>
#include <arpa/inet.h>

#define DATAROW_HEADER  'D'
#define HEADER_SIZE      5
#define HEADER_TYPE_SIZE 1

#define NEED_MORE_INPUT 0x01
#define OTHER_HEADER    0x02

size_t scan_datarows(char *buffer, size_t len, int *reason);
