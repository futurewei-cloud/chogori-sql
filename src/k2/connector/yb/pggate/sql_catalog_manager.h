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

#ifndef CHOGORI_SQL_CATALOG_MANAGER_H
#define CHOGORI_SQL_CATALOG_MANAGER_H

#include "yb/common/env.h"
#include "yb/common/status.h"
#include "yb/common/concurrent/locks.h"
#include "yb/pggate/k2_adapter.h"

using namespace yb;

namespace k2pg {
namespace sql {
    using namespace yb;
    using namespace k2pg::gate;

    class SqlCatalogManager : public std::enable_shared_from_this<SqlCatalogManager>{

    public:
        typedef std::shared_ptr<SqlCatalogManager> SharedPtr;

        SqlCatalogManager(scoped_refptr<K2Adapter> k2_adapter);
        ~SqlCatalogManager();

        CHECKED_STATUS Start();

        virtual void Shutdown();

        virtual Env* GetEnv();

        CHECKED_STATUS IsInitDbDone(bool* isDone);

        void SetCatalogVersion(uint64_t new_version);

        uint64_t GetCatalogVersion() const;

    protected:
        std::atomic<bool> initted_{false};

        mutable simple_spinlock lock_;

    private:
        scoped_refptr<K2Adapter> k2_adapter_;

        std::atomic<bool> init_db_done_{false};

        std::atomic<uint64_t> catalog_version_{0};

    };

} // namespace sql
} // namespace k2pg
#endif //CHOGORI_SQL_CATALOG_MANAGER_H
