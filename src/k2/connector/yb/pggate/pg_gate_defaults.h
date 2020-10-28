/*
MIT License

Copyright(c) 2020 Futurewei Cloud

    Permission is hereby granted,
    free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :

    The above copyright notice and this permission notice shall be included in all copies
    or
    substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS",
    WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
    DAMAGES OR OTHER
    LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/

#ifndef CHOGORI_GATE_DEFAULTS_H
#define CHOGORI_GATE_DEFAULTS_H

namespace k2pg {
namespace gate {
    // Timeout for read, write request from pggate to K2 SKV
    constexpr int default_client_read_write_timeout_ms = 5000;

    // How many read restarts can we try transparently before giving up
    constexpr int32_t default_max_read_restart_attempts = 20;

    // Size of postgres-level output buffer, in bytes.
    constexpr int32_t default_output_buffer_size = 262144;

    // should disable index backfill or not
    static bool const default_disable_index_backfill = true; 

    static const uint64_t default_ysql_prefetch_limit = 4;

    static const uint64_t default_ysql_request_limit = 1;

    static const uint64_t default_ysql_select_parallelism = 1;
    
    static const double default_ysql_backward_prefetch_scale_factor = 0.25;

}  // namespace gate
}  // namespace k2pg

#endif //CHOGORI_GATE_DEFAULTS_H
