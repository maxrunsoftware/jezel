/*
 * Copyright (C) 2022 Max Run Software (dev@maxrunsoftware.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * @param {?string} str - raw html code
 */
function mrsParseHtml(str) {
  if (str === null) {
    return null;
  }
  return $($.parseHTML(str))
}

/**
 * @param {?string} str - html element code
 * @returns {?string} - html element text
 */
function mrsGetElementText(str) {
  const html = mrsParseHtml(str);
  if (html === null) {
    return null
  }
  return html.text();
}

/**
 * @param {?string} str - string to trim
 * @returns {?string} - either the trimmed string, or null if element is null or length 0
 */
function mrsTrimOrNull(str) {
  if (str === null) {
    return null;
  }
  str = str.trim();
  if (str === null) {
    return null;
  }
  if (str.length === 0) {
    return null;
  }
  return str;
}

/**
 * @param {?string} strX - string X
 * @param {?string} strY - string y
 * @param {boolean} trimValues - whether to trim the values first or not
 * @returns {boolean}
 */
function mrsEqualsIgnoreCase(strX, strY, trimValues = false) {
  if (trimValues === true) {
    strX = mrsTrimOrNull(strX);
    strY = mrsTrimOrNull(strY);
  }

  if (strX === strY) {
    return true;
  }

  if (strX === null || strY === null) {
    return false;
  }

  return strX.toUpperCase() === strY.toUpperCase();
}

class Color {
  r = 0;
  g = 0;
  b = 0;

  h = 0;
  s = 0;
  v = 0;

  hex = "";

  constructor(r, g, b, h, s, v, hex) {
    this.r = r;
    this.g = g;
    this.b = b;
    this.h = h;
    this.s = s;
    this.v = v;
    this.hex = hex;
  }

  static fromRGB(r, g, b){
    const hsv = this.rgb2hsv(r / 255, g / 255, b / 255);
    const h = hsv[0], s = hsv[1] * 100, v = hsv[2] * 100;
    return new Color(r, g, b, h, s, v, this.rgb2hex(r, g, b));
  }
  static fromHSV(h, s, v){
    const rgb = this.hsv2rgb(h, s / 100, v / 100);
    const r = rgb[0] * 255, g = rgb[1] * 255, b = rgb[2] * 255;
    return new Color(r, g, b, h, s, v, this.rgb2hex(r, g, b));
  }
  static fromHex(hex) {
    const rgb = this.hex2rgb(hex);
    return this.fromRGB(rgb[0], rgb[1], rgb[2]);
  }
  static fromValue(value){
    // https://stackoverflow.com/a/47355187
    const ctx = document.createElement("canvas").getContext("2d");
    ctx.fillStyle = value;
    const str = String(ctx.fillStyle);
    return this.fromHex(str);
}

  static rgb2hex(r, g, b) {
    // https://stackoverflow.com/a/5624139
    return "#" + ((1 << 24) + (r << 16) + (g << 8) + b).toString(16).slice(1);
  }

  static hex2rgb(hex) {
  // Expand shorthand form (e.g. "03F") to full form (e.g. "0033FF")
  const shorthandRegex = /^#?([a-f\d])([a-f\d])([a-f\d])$/i;
  hex = hex.replace(shorthandRegex, function(m, r, g, b) {
    return r + r + g + g + b + b;
  });

  const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
  return result ? [
    parseInt(result[1], 16),
    parseInt(result[2], 16),
    parseInt(result[3], 16)
  ] : null;
}

  // input: h in [0,360] and s,v in [0,1] - output: r,g,b in [0,1]
  static hsv2rgb(h, s, v)
  {
    // https://stackoverflow.com/a/54024653
    let f= (n,k=(n+h/60)%6) => v - v*s*Math.max( Math.min(k,4-k,1), 0);
    return [f(5),f(3),f(1)];
  }

  // input: r,g,b in [0,1], out: h in [0,360) and s,v in [0,1]
  static rgb2hsv(r, g, b) {
    // https://stackoverflow.com/a/54024653
    let v = Math.max(r,g,b), n = v - Math.min(r,g,b);
    let h= n && ((v==r) ? (g-b)/n : ((v==g) ? 2+(b-r)/n : 4+(r-g)/n));
    return [60*(h<0?h+6:h), v&&n/v, v];
  }

  static
}
