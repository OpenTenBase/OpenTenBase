#ifndef OTB_AIOPS_LLM_CLIENT_H
#define OTB_AIOPS_LLM_CLIENT_H

#include <string>

namespace otb {

struct LlmConfig {
    std::string api_url;
    std::string api_key;
    std::string model;
    int timeout_sec = 60;

    static LlmConfig from_env();
};

struct LlmResponse {
    bool ok = false;
    long http_code = 0;
    std::string content;
    std::string error;
};

class LlmClient {
public:
    static LlmResponse diagnose(const LlmConfig& cfg, const std::string& diagnostic_json);
    static LlmResponse ask(const LlmConfig& cfg, const std::string& question,
                           const std::string& diagnostic_json);
};

}  // namespace otb

#endif  // OTB_AIOPS_LLM_CLIENT_H
