/* Copyright (C) 2026 OpenTenBase Authors. All rights reserved.
 * See the COPYRIGHT file for license information.
 */

// Keep CLI.hpp first: another header must not supply its integer types.
#include "CLI/CLI.hpp"

int main()
{
    CLI::AsSizeValue binary(false);
    CLI::AsSizeValue decimal(true);
    std::string kib = "1 KiB";
    std::string kb = "1 KB";
    std::string eib = "2 EiB";

    if (!binary(kib).empty() || kib != "1024" ||
        !decimal(kb).empty() || kb != "1000" ||
        !binary(eib).empty() || eib != "2305843009213693952")
    {
        std::cerr << "size validator regression\n";
        return 1;
    }
    std::cout << "CLI header and size validator tests passed\n";
    return 0;
}
