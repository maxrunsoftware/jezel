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
package com.maxrunsoftware.jezel.view;

import static com.maxrunsoftware.jezel.Util.*;

import java.io.IOException;

import com.maxrunsoftware.jezel.Version;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class HomeServlet extends ServletBase {
	private static final long serialVersionUID = 3792719800292236528L;

	@Override
	protected void doGetAuthorized(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		var json = createObjectBuilder()
				.add(RESPONSE_STATUS, RESPONSE_STATUS_SUCCESS)
				.add(RESPONSE_MESSAGE, "Jezel v" + Version.VALUE);

		writeResponse(response, json);
	}
}
