// Copyright (c) 2024 Max Run Software (dev@maxrunsoftware.com)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

namespace MaxRunSoftware.Jezel.Common.Data;

public interface IModel
{
    public Guid GetPrimaryKey();
    public void SetPrimaryKey(Guid id);
}

public class Job : IModel
{
    public Guid JobId { get; set; } = Guid.Empty;
    public bool IsActive { get; set; } = true;
    public string? Name { get; set; }
    public string? Description { get; set; }
    public bool IsDeleted { get; set; } = false;
    public bool IsExecutionSnapshot { get; set; } = false;

    public Guid GetPrimaryKey() => JobId;
    public void SetPrimaryKey(Guid id) => JobId = id;
}

public class JobTag : IModel
{
    public Guid JobTagId { get; set; } = Guid.Empty;
    public Guid JobId { get; set; } = Guid.Empty;
    public string? Name { get; set; }

    public Guid GetPrimaryKey() => JobTagId;
    public void SetPrimaryKey(Guid id) => JobTagId = id;
}

public class JobSchedule : IModel
{
    public Guid JobScheduleId { get; set; } = Guid.Empty;
    public Guid JobId { get; set; } = Guid.Empty;
    public bool IsActive { get; set; } = true;
    public string? Name { get; set; }
    public string? Description { get; set; }
    public string Cron { get; set; } = string.Empty;

    public Guid GetPrimaryKey() => JobScheduleId;
    public void SetPrimaryKey(Guid id) => JobScheduleId = id;
}

public class JobStep : IModel
{
    public Guid JobStepId { get; set; } = Guid.Empty;
    public Guid JobId { get; set; } = Guid.Empty;
    public bool IsActive { get; set; } = true;
    public string? Name { get; set; }
    public string? Description { get; set; }
    public int Index { get; set; } = int.MinValue;
    public string? Type { get; set; }
    public string? Data { get; set; }

    public Guid GetPrimaryKey() => JobStepId;
    public void SetPrimaryKey(Guid id) => JobStepId = id;
}

public class JobExecution : IModel
{
    public Guid JobExecutionId { get; set; } = Guid.Empty;
    public Guid JobId { get; set; } = Guid.Empty;
    public Guid SnapshotJobId { get; set; } = Guid.Empty;
    public DateTimeOffset? QueuedOn { get; set; }
    public DateTimeOffset? StartedOn { get; set; }
    public DateTimeOffset? CompletedOn { get; set; }
    public DateTimeOffset? CancelledOn { get; set; }
    public Guid? CancelledUserId { get; set; }
    public string? Status { get; set; }
    public Guid? TriggerJobScheduleId { get; set; } = Guid.Empty;
    public Guid? TriggerUserId { get; set; } = Guid.Empty;

    public Guid GetPrimaryKey() => JobExecutionId;
    public void SetPrimaryKey(Guid id) => JobExecutionId = id;
}

public class JobExecutionStep : IModel
{
    public Guid JobExecutionStepId { get; set; } = Guid.Empty;
    public Guid JobExecutionId { get; set; } = Guid.Empty;
    public Guid SnapshotJobStepId { get; set; } = Guid.Empty;
    public DateTimeOffset? StartedOn { get; set; }
    public DateTimeOffset? CompletedOn { get; set; }
    public string? Status { get; set; }

    public Guid GetPrimaryKey() => JobExecutionStepId;
    public void SetPrimaryKey(Guid id) => JobExecutionStepId = id;
}
