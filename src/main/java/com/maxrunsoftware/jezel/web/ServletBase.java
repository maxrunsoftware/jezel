/*
 * Copyright (c) 2021 Max Run Software (dev@maxrunsoftware.com)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.maxrunsoftware.jezel.web;

import static com.maxrunsoftware.jezel.Util.*;

import java.io.IOException;

import com.maxrunsoftware.jezel.Constant;
import com.maxrunsoftware.jezel.SettingService;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletResponse;

public abstract class ServletBase extends com.maxrunsoftware.jezel.util.ServletBase {
	private static final long serialVersionUID = 7162466372715656028L;
	private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(ServletBase.class);
	private static final String CSS = """
			.topnav {
			  background-color: #333;
			  overflow: hidden;
			}

			/* Style the links inside the navigation bar */
			.topnav a {
			  float: left;
			  color: #f2f2f2;
			  text-align: center;
			  padding: 14px 16px;
			  text-decoration: none;
			  font-size: 17px;
			}

			/* Change the color of links on hover */
			.topnav a:hover {
			  background-color: #ddd;
			  color: black;
			}

			/* Add a color to the active/current link */
			.topnav a.active {
			  background-color: #04AA6D;
			  color: white;
			}


			table {
			  font-family: Arial, Helvetica, sans-serif;
			  border-collapse: collapse;
			  width: 100%;
			  table-layout: fixed;
			}
			th {
			  border: 1px solid #ddd;
			  padding: 8px;
			  width: 1px;
			  white-space: nowrap;
			  padding-top: 12px;
			  padding-bottom: 12px;
			  text-align: left;
			  background-color: #4CAF50;
			  color: white;
			  cursor: pointer;
			}
			td {
			  border: 1px solid #ddd;
			  padding: 8px;
			  width: 1px;
			  white-space: normal;
			  word-break: break-word;
			  text-wrap: normal;
			}


			tr:nth-child(even){background-color: #f2f2f2;}
			tr:hover {background-color: #ddd;}

			.errorMessage {
			  font-weight: bold;
			  font-size: 32px;
			  color: red;
			}

			""";

	private static final String JAVASCRIPT = """

			""";

	protected SettingService settings;
	protected DataService data;

	@Override
	public void init() throws ServletException {
		settings = getResource(SettingService.class);
		data = getResource(DataService.class);
	}

	protected void writeResponse(HttpServletResponse response, String html) {
		writeResponse(response, trimOrNull(getClass().getSimpleName().replace("Servlet", "")), html);
	}

	protected void writeResponse(HttpServletResponse response, String title, String html) {
		writeResponse(response, title, html, 200);
	}

	protected static enum Nav {
		NONE, HOME, JOBS, SCHEDULES, LOGS, CONFIG, LOGOUT
	}

	protected abstract Nav getNav();

	protected void writeResponse(HttpServletResponse response, String title, String html, int statusCode) {
		html = coalesce(trimOrNull(html), "Missing HTML");
		var topNav = """
				<div class="topnav">
					<a ${activeHome} href="/">Home</a>
					<a ${activeJobs} href="/jobs">Jobs</a>
					<a ${activeSchedules} href="/schedules">Schedules</a>
					<a ${activeLogs} href="/logs">Logs</a>
					<a ${activeConfig} href="/config">Configuration</a>
					<a ${activeLogout} href="/logout">Logout</a>
				</div>
				""";
		var active = "class=\"active\"";
		topNav = topNav.replace("${activeHome}", getNav().equals(Nav.HOME) ? active : "");
		topNav = topNav.replace("${activeJobs}", getNav().equals(Nav.JOBS) ? active : "");
		topNav = topNav.replace("${activeSchedules}", getNav().equals(Nav.SCHEDULES) ? active : "");
		topNav = topNav.replace("${activeLogs}", getNav().equals(Nav.LOGS) ? active : "");
		topNav = topNav.replace("${activeConfig}", getNav().equals(Nav.CONFIG) ? active : "");
		topNav = topNav.replace("${activeLogout}", getNav().equals(Nav.LOGOUT) ? active : "");

		var str = """
				<html dir="ltr" lang="en">
					<head>
						<meta charset="utf-8">
						<title>${title}</title>
						<style>${style}</style>
						<script>${script}</script>
					</head>
					<body>
						${topNav}
						<br>

						${body}
					</body>
				</html>
				""";

		str = str.replace("${title}", title);
		str = str.replace("${style}", CSS);
		str = str.replace("${script}", JAVASCRIPT);
		str = str.replace("${body}", html);
		str = str.replace("${topNav}", topNav);
		str = trimOrNull(str);
		html = str;

		LOG.trace("Writing response [" + statusCode + "]: " + html);
		response.setContentType(Constant.CONTENTTYPE_HTML);
		response.setCharacterEncoding(Constant.ENCODING_UTF8);
		response.setStatus(statusCode);
		response.addHeader("Cache-Control", "no-cache");
		response.addHeader("Content-Language", "en-US");
		try {
			response.getWriter().print(html);
		} catch (IOException ioe) {
			LOG.error("Error writing response", ioe);
		}
	}

}
