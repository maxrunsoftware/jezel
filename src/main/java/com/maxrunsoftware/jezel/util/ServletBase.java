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
package com.maxrunsoftware.jezel.util;

import static com.maxrunsoftware.jezel.Util.*;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.hc.core5.net.URIBuilder;

import com.maxrunsoftware.jezel.Constant;

import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public abstract class ServletBase extends HttpServlet {
	private static final long serialVersionUID = 2411514340765948727L;
	private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(ServletBase.class);
	private Object locker = new Object();
	private Map<String, Object> resources = null;

	@SuppressWarnings("unchecked")
	protected <T> T getResource(Class<T> clazz) {
		synchronized (locker) {
			if (resources == null) {
				resources = new CaseInsensitiveMap<String, Object>();

				var ctx = getServletContext();
				for (var attrName : Collections.list(ctx.getAttributeNames())) {
					var attrVal = ctx.getAttribute(attrName);
					if (attrVal != null) {
						LOG.trace("Found attribute [" + attrName + "]: " + attrVal.getClass().getName());
						resources.put(attrName, attrVal);
					}
				}
			}
			LOG.debug("Getting service " + clazz.getName());
			var o = resources.get(clazz.getName());
			if (o == null) throw new IllegalArgumentException("No service found named " + clazz.getName());
			return (T) o;
		}
	}

	protected static String parameters(
			String name1,
			Object value1

	) {
		return new URIBuilder()
				.addParameter(name1, value1 == null ? "" : value1.toString())
				.toString();
	}

	protected static String parameters(
			String name1,
			Object value1,
			String name2,
			Object value2

	) {
		return new URIBuilder()
				.addParameter(name1, value1 == null ? "" : value1.toString())
				.addParameter(name2, value2 == null ? "" : value2.toString())
				.toString();
	}

	protected static String parameters(
			String name1,
			Object value1,
			String name2,
			Object value2,
			String name3,
			Object value3

	) {
		return new URIBuilder()
				.addParameter(name1, value1 == null ? "" : value1.toString())
				.addParameter(name2, value2 == null ? "" : value2.toString())
				.addParameter(name3, value3 == null ? "" : value3.toString())
				.toString();
	}

	protected static String parameters(
			String name1,
			Object value1,
			String name2,
			Object value2,
			String name3,
			Object value3,
			String name4,
			Object value4

	) {
		return new URIBuilder()
				.addParameter(name1, value1 == null ? "" : value1.toString())
				.addParameter(name2, value2 == null ? "" : value2.toString())
				.addParameter(name3, value3 == null ? "" : value3.toString())
				.addParameter(name4, value4 == null ? "" : value4.toString())
				.toString();
	}

	protected static Integer getParameterInt(HttpServletRequest request, String name) {
		String val = trimOrNull(getParameter(request, name));
		if (val == null) return null;
		try {
			return Integer.parseInt(val);
		} catch (Throwable t) {
			return null;
		}
	}

	protected static boolean getParameterBool(HttpServletRequest request, String name) {
		String val = trimOrNull(getParameter(request, name));
		if (val == null) return false;
		try {
			return parseBoolean(val);
		} catch (Throwable t) {
			return false;
		}
	}

	protected static String getParameter(HttpServletRequest request, String name) {
		name = trimOrNull(name);
		if (name == null) return null;
		name = name.toLowerCase();

		for (var pName : Collections.list(request.getParameterNames())) {
			if (pName.toLowerCase().equals(name)) {
				var val = request.getParameter(pName);
				LOG.debug("Found header parameter [" + pName + "]: " + val);
				return request.getParameter(pName);
			}
		}
		return null;
	}

	protected static void writeResponse(HttpServletResponse response, String content, int statusCode, String contentType) {
		LOG.trace("Writing response [" + statusCode + "]: " + content);
		response.setContentType(contentType);
		response.setCharacterEncoding(Constant.ENCODING_UTF8);
		response.setStatus(statusCode);
		response.addHeader("Cache-Control", "no-cache");
		response.addHeader("Content-Language", "en-US");
		try {
			response.getWriter().print(content);
		} catch (IOException ioe) {
			LOG.error("Error writing response", ioe);
		}
	}

}
