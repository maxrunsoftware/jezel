#  Copyright (C) 2022 Max Run Software (dev@maxrunsoftware.com)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from __future__ import annotations

from enum import Enum
from typing import List

from bs4 import BeautifulSoup
import flask
from markupsafe import Markup
from werkzeug.datastructures import MultiDict

from config import Config
from utils import DictStrCasefold, trim, trim_casefold, xstr


# https://getbootstrap.com/docs/5.0/components/alerts/
class BootstrapColor(str, Enum):
    PRIMARY = "primary"
    SECONDARY = "secondary"
    SUCCESS = "success"
    INFO = "info"
    WARNING = "warning"
    DANGER = "danger"
    LIGHT = "light"
    DARK = "dark"

    BLUE = PRIMARY
    BLUE_LIGHT = INFO
    GRAY = SECONDARY
    GRAY_LIGHT = LIGHT
    GRAY_DARK = DARK
    RED = DANGER
    YELLOW = WARNING
    GREEN = SUCCESS

    def __str__(self): return self.value

    @property
    def text(self):
        return f"text-{self}"

    @property
    def button(self):
        return f"btn-{self}"

    @classmethod
    def colors(cls) -> List[BootstrapColor]: return [
        BootstrapColor.PRIMARY,
        BootstrapColor.SECONDARY,
        BootstrapColor.SUCCESS,
        BootstrapColor.INFO,
        BootstrapColor.WARNING,
        BootstrapColor.DANGER,
        BootstrapColor.LIGHT,
        BootstrapColor.DARK,
    ]

    @classmethod
    def colors_str(cls): return [str(color) for color in cls.colors()]


# https://cdn.jsdelivr.net/npm/bootstrap-icons/font/bootstrap-icons.css
class BootstrapIcon(str, Enum):
    CHECK_CIRCLE_FILL = "check-circle-fill"
    INFO_FILL = "info-fill"
    EXCLAMATION_TRIANGLE_FILL = "exclamation-triangle-fill"
    POWER = "power"
    LIGHTBULB = "lightbulb"
    LIGHTBULB_FILL = "lightbulb-fill"
    LIGHTBULB_OFF = "lightbulb-off"
    LIGHTBULB_OFF_FILL = "lightbulb-off-fill"

    def __str__(self): return f"bi-{self.value}"


def flash(message: str, category: BootstrapColor | str = BootstrapColor.PRIMARY, icon: BootstrapIcon | None = None):
    if icon is not None:
        icon_clazz_items = [
            "bi",
            "{icon}",
            "flex-shrink-0",
            "me-2",
            "d-inline-flex",
            "align-items-center",
            "align-middle",
        ]
        icon_style_items = {
            "font-size": "1.5rem",
            "margin-top": "-0.27rem",
        }
        clazz = ' '.join([x.replace("{icon}", str(icon)) for x in icon_clazz_items])
        style = ' '.join([(k + ": " + v + ("" if v.endswith(";") else ";")) for k, v in icon_style_items.items()])
        icon_markup = Markup('<i class="{clazz}" style="{style}" aria-hidden="true"></i>'.format(clazz=clazz, style=style))
        message = icon_markup + message

    flask.flash(message=message, category=category)


def render(html):
    if Config.WEB_PRETTY_HTML:
        soup = BeautifulSoup(html, "html5lib")
        html = soup.prettify(formatter="html")
    return html


def join_classes(*argv) -> str:
    result = []
    items = [arg for arg in argv]
    while len(items) > 0:
        item = items.pop()
        if item is None: continue

        if isinstance(item, str):
            itm = trim(str(item))
            if itm is not None:
                result.append(itm)
        else:
            try:
                iterator = iter(item)
            except TypeError:
                pass  # not iterable
            else:
                for newitem in iterator:
                    items.append(newitem)

    return " ".join(result)


def convert_multidic_lists(md: MultiDict, casefold_values: bool = False) -> DictStrCasefold[str, [str]]:
    d = DictStrCasefold()
    if md is None: return d
    for k, lis in md.lists():
        k = trim(xstr(k))
        if k is None: continue
        if lis is None: continue
        if len(lis) == 0: continue
        lis_new = []
        for item in lis:
            v = xstr(item)
            v = trim_casefold(v) if casefold_values else trim(v)
            if v is not None: lis_new.append(v)
        if len(lis) > 0:
            d[k] = lis_new
    return d


def convert_multidic(md: MultiDict, casefold_values: bool = False) -> DictStrCasefold[str, str]:
    d = DictStrCasefold()
    for k, lis in convert_multidic_lists(md, casefold_values=casefold_values).items():
        d[k] = lis[0]
    return d
