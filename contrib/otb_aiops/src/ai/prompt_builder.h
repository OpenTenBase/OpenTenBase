#ifndef OTB_AIOPS_PROMPT_BUILDER_H
#define OTB_AIOPS_PROMPT_BUILDER_H

#include <string>

namespace otb {

std::string build_diagnose_system_prompt();
std::string build_ask_system_prompt();
std::string build_ask_user_prompt(const std::string& question, const std::string& diagnostic_json);

}  // namespace otb

#endif  // OTB_AIOPS_PROMPT_BUILDER_H
