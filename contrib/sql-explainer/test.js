/*
 * test.js - regression checks for sql_explainer.html
 * Copyright (c) 2026 OpenTenBase Authors. All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * Run with: node test.js   (no dependencies, no database required)
 */
'use strict';

const fs = require('fs');
const path = require('path');

const html = fs.readFileSync(path.join(__dirname, 'sql_explainer.html'), 'utf8');
const script = html.split('<script>')[1].split('</script>')[0];

let checks = 0;
let failures = 0;

function check(name, cond) {
  checks++;
  if (cond) {
    console.log('PASS  ' + name);
  } else {
    console.log('FAIL  ' + name);
    failures++;
  }
}

check('BSD-3-Clause header present', html.includes('SPDX-License-Identifier: BSD-3-Clause'));
check('no mis-pasted fragments remain', !html.includes('列名；col1 也可作为 HASH 分布键'));

let syntaxOk = true;
try {
  new Function(script);
} catch (e) {
  syntaxOk = false;
  console.log('      syntax error: ' + e.message);
}
check('inline script parses', syntaxOk);

const dSrc = script.match(/const D=\[[\s\S]*?\n\];/);
check('rule table found', !!dSrc);
if (!dSrc) {
  process.exit(1);
}

const D = new Function(dSrc[0] + '; return D;')();

function ruleFor(text) {
  const hit = D.find(function (x) { return x.re.test(text); });
  return hit ? hit.name : null;
}

const NL = String.fromCharCode(10);
const TAB = String.fromCharCode(9);

const cases = [
  ['CREATE TABLE products (product_no integer, name text, price numeric);', 'CREATE TABLE'],
  ["INSERT INTO products VALUES (1, 'Cheese', 9.99);", 'INSERT INTO'],
  ['SELECT * FROM table1;', 'SELECT'],
  ['DISTRIBUTE BY HASH(col1)', 'DISTRIBUTE BY HASH'],
  ['UPDATE products SET price = 5 WHERE price = 9.99;', 'UPDATE'],
  ['product_no integer', '列定义'],
  // A parenthesised row is intentionally more specific than the VALUES keyword.
  ["VALUES (3, 'Milk', 2.99)", '单行数据组'],
  ['VALUES', 'VALUES'],
  ['WHERE price = 5', 'WHERE'],
  ['*', '星号 *'],
  ['CREATE' + NL + 'TABLE t (a int);', 'CREATE TABLE'],
  ['CREATE' + TAB + 'TABLE t (a int);', 'CREATE TABLE']
];

for (const c of cases) {
  check('resolves ' + JSON.stringify(c[0].slice(0, 42)) + ' -> ' + c[1], ruleFor(c[0]) === c[1]);
}

const m = script.match(/var isCol=\/(\^.*?)\/i\.test\(sel\)/);
check('column-detection regex found', !!m);
if (m) {
  const re = new RegExp(m[1], 'i');
  check('column-detection regex matches a column definition', re.test('product_no integer'));
  check('column-detection regex rejects unrelated text', !re.test('CREATE TABLE'));
}

console.log('');
console.log(checks + ' checks, ' + failures + ' failed');
process.exit(failures ? 1 : 0);
