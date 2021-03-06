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
package com.maxrunsoftware.jezel.action;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.map.CaseInsensitiveMap;

public abstract class CommandBase implements Command {
	private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(CommandBase.class);

	private final Map<String, String> parameters = new CaseInsensitiveMap<String, String>();
	protected CommandLog log;

	@Override
	public void setParameters(Map<String, String> parameters) {
		this.parameters.clear();
		var sb = new StringBuilder();
		sb.append("Setting parameters on Command to {");

		for (var key : parameters.keySet()) {
			var val = parameters.get(key);
			if (val != null) {
				sb.append("  " + key + ": " + val);
				this.parameters.put(key, val);
			}
		}
		sb.append("}");
		LOG.debug(sb.toString());
	}

	@Override
	public void setLog(CommandLog log) {
		this.log = log;
	}

	@Override
	public List<CommandParameter> getParameterDetails() {
		var list = new ArrayList<CommandParameter>();
		addParameterDetails(list);
		for (var item : list) {
			item.setClazz(getClass().getSimpleName());
		}
		return list;
	}

	protected abstract void addParameterDetails(List<CommandParameter> l);

	protected String getParameter(String name) {
		return parameters.get(name);
	}

	protected String getParameterRequired(String name) {
		var p = getParameter(name);
		if (p == null) throw new IllegalArgumentException("Parameter [" + name + "] is not defined");
		return p;
	}

}
