//
// THE SOFTWARE IS PROVIDED "AS IS",
// WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
//        AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
//        DAMAGES OR OTHER LIABILITY,
// WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
//
#include "k2_config.h"
#include "k2_includes.h"

#include <stdlib.h>

#include <fstream>

namespace k2pg {
namespace gate {

Config::Config() {
    const char* configFileName = getenv("K2_CONFIG_FILE");
    if (NULL == configFileName) {
        K2LOG_W(log::k2Client, "No config file given for K2 configuration in the K2_CONFIG_FILE env variable");
        return;
    }

    // read the config file
    K2LOG_I(log::k2Client, "{}", configFileName);
    std::ifstream ifile(configFileName);
    ifile >> _config;
}

Config::~Config(){
}

}
}
