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

import static com.maxrunsoftware.jezel.action.CommandParameter.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.maxrunsoftware.jezel.util.Table;

public class SqlQuery extends CommandBase {

	@Override
	public void execute() throws Exception {
		var connectionString = getParameterRequired("ConnectionString");
		var sql = getParameterRequired("SQL");

		// connectionString =
		// "jdbc:sqlserver://yourserver.database.windows.net:1433;database=AdventureWorks;user=yourusername@yourserver;password=yourpassword;loginTimeout=30;";

		try (Connection connection = DriverManager.getConnection(connectionString)) {
			var statement = connection.prepareStatement(sql);

			var tables = Table.parse(statement);

		}

	}

	@Override
	protected void addParameterDetails(List<CommandParameter> l) {
		l.add(createString("ConnectionString", "The JDBC connection string"));
		l.add(createText("SQL", "The SQL Statement(s) to execute"));
		for (int i = 1; i <= 20; i++) {
			var iString = StringUtils.right("00" + i, 2);
			l.add(createFilename("OutputFile" + iString, "The tab-delimited output " + iString + " file"));
		}

	}

}
