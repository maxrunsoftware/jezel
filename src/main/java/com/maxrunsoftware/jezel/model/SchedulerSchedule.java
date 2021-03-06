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
package com.maxrunsoftware.jezel.model;

import static com.maxrunsoftware.jezel.Util.*;

import java.util.Comparator;

import javax.json.JsonObject;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;

import com.maxrunsoftware.jezel.JsonCodable;

@Entity
public class SchedulerSchedule implements JsonCodable {

	public static final String NAME = "schedulerSchedule";
	public static final String ID = NAME + "Id";

	public static final Comparator<SchedulerSchedule> SORT_ID = new Comparator<SchedulerSchedule>() {
		@Override
		public int compare(SchedulerSchedule o1, SchedulerSchedule o2) {
			if (o1 == o2) return 0;
			if (o1 == null) return -1;
			if (o2 == null) return 1;
			return Integer.valueOf(o1.getSchedulerScheduleId()).compareTo(o2.getSchedulerScheduleId());
		}
	};

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private int schedulerScheduleId;

	public int getSchedulerScheduleId() {
		return schedulerScheduleId;
	}

	public void setSchedulerScheduleId(int schedulerScheduleId) {
		this.schedulerScheduleId = schedulerScheduleId;
	}

	@ManyToOne
	@JoinColumn(name = SchedulerJob.ID, nullable = false, referencedColumnName = SchedulerJob.ID)
	private SchedulerJob schedulerJob;

	public SchedulerJob getSchedulerJob() {
		return schedulerJob;
	}

	public void setSchedulerJob(SchedulerJob schedulerJob) {
		this.schedulerJob = schedulerJob;
	}

	@Column(nullable = false)
	private boolean sunday;

	public boolean isSunday() {
		return sunday;
	}

	public void setSunday(boolean sunday) {
		this.sunday = sunday;
	}

	@Column(nullable = false)
	private boolean monday;

	public boolean isMonday() {
		return monday;
	}

	public void setMonday(boolean monday) {
		this.monday = monday;
	}

	@Column(nullable = false)
	private boolean tuesday;

	public boolean isTuesday() {
		return tuesday;
	}

	public void setTuesday(boolean tuesday) {
		this.tuesday = tuesday;
	}

	@Column(nullable = false)
	private boolean wednesday;

	public boolean isWednesday() {
		return wednesday;
	}

	public void setWednesday(boolean wednesday) {
		this.wednesday = wednesday;
	}

	@Column(nullable = false)
	private boolean thursday;

	public boolean isThursday() {
		return thursday;
	}

	public void setThursday(boolean thursday) {
		this.thursday = thursday;
	}

	@Column(nullable = false)
	private boolean friday;

	public boolean isFriday() {
		return friday;
	}

	public void setFriday(boolean friday) {
		this.friday = friday;
	}

	@Column(nullable = false)
	private boolean saturday;

	public boolean isSaturday() {
		return saturday;
	}

	public void setSaturday(boolean saturday) {
		this.saturday = saturday;
	}

	@Column(nullable = false)
	private int hour;

	public int getHour() {
		return hour;
	}

	public void setHour(int hour) {
		if (hour > 23) hour = 23;
		if (hour < 0) hour = 0;
		this.hour = hour;
	}

	@Column(nullable = false)
	private int minute;

	public int getMinute() {
		return minute;
	}

	public void setMinute(int minute) {
		if (minute > 59) minute = 59;
		if (minute < 0) minute = 0;
		this.minute = minute;
	}

	@Column(nullable = false)
	private boolean disabled;

	public boolean isDisabled() {
		return disabled;
	}

	public void setDisabled(boolean disabled) {
		this.disabled = disabled;
	}

	@Override
	public JsonObject toJson() {
		var json = createObjectBuilder();
		json.add(ID, getSchedulerScheduleId());
		json.add(SchedulerJob.ID, getSchedulerJob().getSchedulerJobId());
		json.add("sunday", isSunday());
		json.add("monday", isMonday());
		json.add("tuesday", isTuesday());
		json.add("wednesday", isWednesday());
		json.add("thursday", isThursday());
		json.add("friday", isFriday());
		json.add("saturday", isSaturday());
		json.add("hour", getHour());
		json.add("minute", getMinute());
		json.add("disabled", isDisabled());
		return json.build();
	}

	@Override
	public void fromJson(JsonObject o) {
		this.setSchedulerScheduleId(o.getInt(ID));
		this.setSunday(o.getBoolean("sunday"));
		this.setMonday(o.getBoolean("monday"));
		this.setTuesday(o.getBoolean("tuesday"));
		this.setWednesday(o.getBoolean("wednesday"));
		this.setThursday(o.getBoolean("thursday"));
		this.setFriday(o.getBoolean("friday"));
		this.setSaturday(o.getBoolean("saturday"));
		this.setHour(o.getInt("hour"));
		this.setMinute(o.getInt("minute"));
		this.setDisabled(o.getBoolean("disabled"));
	}

	public void setDays(
			boolean sunday,
			boolean monday,
			boolean tuesday,
			boolean wednesday,
			boolean thursday,
			boolean friday,
			boolean saturday) {
		setSunday(sunday);
		setMonday(monday);
		setTuesday(tuesday);
		setWednesday(wednesday);
		setThursday(thursday);
		setFriday(friday);
		setSaturday(saturday);
	}

	public void setTime(int hour, int minute) {
		setHour(hour);
		setMinute(minute);
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + "[" + getSchedulerScheduleId() + "]";
	}
}
