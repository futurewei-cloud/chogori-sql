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

#include "yb/pggate/cluster_info_handler.h"

namespace k2pg {
namespace sql {

ClusterInfoHandler::ClusterInfoHandler(scoped_refptr<K2Adapter> k2_adapter) : k2_adapter_(k2_adapter) {
}

ClusterInfoHandler::~ClusterInfoHandler() {
}

Status ClusterInfoHandler::CreateClusterInfo(ClusterInfo& cluster_info) {

    return Status::OK();
}

Status ClusterInfoHandler::UpdateClusterInfo(ClusterInfo& cluster_info) {

    return Status::OK();
}

Status ClusterInfoHandler::ReadClusterInfo(ClusterInfo& cluster_info) {
    
    return Status::OK();
}

} // namespace sql
} // namespace k2pg
