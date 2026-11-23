#include "ai/llm_client.h"
#include "ai/prompt_builder.h"
#include "formatter/formatter.h"

#include <curl/curl.h>
#include <cstdlib>
#include <sstream>

namespace otb {

namespace {

std::string env_or(const char* name, const std::string& fallback) {
    const char* v = std::getenv(name);
    return (v && *v) ? std::string(v) : fallback;
}

size_t curl_write_cb(char* ptr, size_t size, size_t nmemb, void* userdata) {
    auto* out = static_cast<std::string*>(userdata);
    out->append(ptr, size * nmemb);
    return size * nmemb;
}

// Extract the string value of the first "content" field inside "choices" from
// an OpenAI-compatible chat completion JSON response.
bool extract_openai_content(const std::string& body, std::string& out) {
    size_t choices_pos = body.find("\"choices\"");
    size_t search_start = (choices_pos != std::string::npos) ? choices_pos : 0;

    size_t key_pos = body.find("\"content\"", search_start);
    if (key_pos == std::string::npos) {
        return false;
    }
    size_t colon = body.find(':', key_pos + 9);
    if (colon == std::string::npos) {
        return false;
    }
    size_t i = colon + 1;
    while (i < body.size() && (body[i] == ' ' || body[i] == '\t' || body[i] == '\n' || body[i] == '\r')) {
        ++i;
    }
    if (i >= body.size() || body[i] != '\"') {
        return false;
    }
    ++i;  // skip opening quote

    std::string decoded;
    while (i < body.size()) {
        char c = body[i++];
        if (c == '\"') {
            out = std::move(decoded);
            return true;
        }
        if (c == '\\' && i < body.size()) {
            char esc = body[i++];
            switch (esc) {
                case '\"': decoded.push_back('\"'); break;
                case '\\': decoded.push_back('\\'); break;
                case '/':  decoded.push_back('/'); break;
                case 'b':  decoded.push_back('\b'); break;
                case 'f':  decoded.push_back('\f'); break;
                case 'n':  decoded.push_back('\n'); break;
                case 'r':  decoded.push_back('\r'); break;
                case 't':  decoded.push_back('\t'); break;
                default:   decoded.push_back(esc); break;
            }
        } else {
            decoded.push_back(c);
        }
    }
    return false;
}

}  // namespace

LlmConfig LlmConfig::from_env() {
    LlmConfig cfg;
    cfg.api_url = env_or("OTB_AI_BASE_URL", "https://api.openai.com/v1/chat/completions");
    cfg.api_key = env_or("OTB_AI_API_KEY", env_or("OPENAI_API_KEY", ""));
    cfg.model = env_or("OTB_AI_MODEL", "gpt-4o-mini");
    return cfg;
}

namespace {

LlmResponse send_chat_completion(const LlmConfig& cfg,
                                 const std::string& system_prompt,
                                 const std::string& user_prompt) {
    LlmResponse resp;
    if (cfg.api_key.empty()) {
        resp.ok = false;
        resp.error = "API key is not set. Export OTB_AI_API_KEY (or pass --api-key).";
        return resp;
    }

    std::ostringstream payload;
    payload << "{"
            << "\"model\":\"" << json_escape(cfg.model) << "\","
            << "\"temperature\":0.2,"
            << "\"messages\":["
            << "{\"role\":\"system\",\"content\":\"" << json_escape(system_prompt) << "\"},"
            << "{\"role\":\"user\",\"content\":\"" << json_escape(user_prompt) << "\"}"
            << "]"
            << "}";
    std::string payload_str = payload.str();

    CURL* curl = curl_easy_init();
    if (!curl) {
        resp.ok = false;
        resp.error = "Failed to initialize libcurl";
        return resp;
    }

    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    std::string auth = "Authorization: Bearer " + cfg.api_key;
    headers = curl_slist_append(headers, auth.c_str());

    std::string raw_body;
    curl_easy_setopt(curl, CURLOPT_URL, cfg.api_url.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payload_str.c_str());
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, static_cast<long>(payload_str.size()));
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, cfg.timeout_sec);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &raw_body);

    CURLcode res = curl_easy_perform(curl);
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &resp.http_code);

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);

    if (res != CURLE_OK) {
        resp.ok = false;
        resp.error = std::string("HTTP request failed: ") + curl_easy_strerror(res);
        return resp;
    }

    if (resp.http_code < 200 || resp.http_code >= 300) {
        resp.ok = false;
        resp.error = "LLM API returned HTTP " + std::to_string(resp.http_code) + ": " + raw_body;
        return resp;
    }

    if (!extract_openai_content(raw_body, resp.content)) {
        resp.ok = false;
        resp.error = "Failed to parse choices[0].message.content from LLM response: " + raw_body;
        return resp;
    }

    resp.ok = true;
    return resp;
}

}  // namespace

LlmResponse LlmClient::diagnose(const LlmConfig& cfg, const std::string& diagnostic_json) {
    return send_chat_completion(cfg, build_diagnose_system_prompt(), diagnostic_json);
}

LlmResponse LlmClient::ask(const LlmConfig& cfg, const std::string& question,
                           const std::string& diagnostic_json) {
    return send_chat_completion(cfg, build_ask_system_prompt(),
                                build_ask_user_prompt(question, diagnostic_json));
}

}  // namespace otb
