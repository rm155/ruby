#include "prism/enc/pm_encoding.h"

static size_t
pm_encoding_big5_char_width(const uint8_t *b, ptrdiff_t n) {
    // These are the single byte characters.
    if (*b < 0x80) {
        return 1;
    }

    // These are the double byte characters.
    if ((n > 1) && (b[0] >= 0xA1 && b[0] <= 0xFE) && (b[1] >= 0x40 && b[1] <= 0xFE)) {
        return 2;
    }

    return 0;
}

static size_t
pm_encoding_big5_alpha_char(const uint8_t *b, ptrdiff_t n) {
    if (pm_encoding_big5_char_width(b, n) == 1) {
        return pm_encoding_ascii_alpha_char(b, n);
    } else {
        return 0;
    }
}

static size_t
pm_encoding_big5_alnum_char(const uint8_t *b, ptrdiff_t n) {
    if (pm_encoding_big5_char_width(b, n) == 1) {
        return pm_encoding_ascii_alnum_char(b, n);
    } else {
        return 0;
    }
}

static bool
pm_encoding_big5_isupper_char(const uint8_t *b, ptrdiff_t n) {
    if (pm_encoding_big5_char_width(b, n) == 1) {
        return pm_encoding_ascii_isupper_char(b, n);
    } else {
        return false;
    }
}

pm_encoding_t pm_encoding_big5 = {
    .name = "big5",
    .char_width = pm_encoding_big5_char_width,
    .alnum_char = pm_encoding_big5_alnum_char,
    .alpha_char = pm_encoding_big5_alpha_char,
    .isupper_char = pm_encoding_big5_isupper_char,
    .multibyte = true
};
