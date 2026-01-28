export type JobStatus =
  | "pending"
  | "analyzing"
  | "processing"
  | "completed"
  | "failed";

export type DataSourceType = "file" | "api" | "cloud";

export type FieldIssue = {
  field: string;
  level: "warning" | "error";
  message: string;
};

export type IngestionJob = {
  id: string;
  name: string;
  sourceType: DataSourceType;
  createdAt: string;
  status: JobStatus;
  records: number;
  issues: FieldIssue[];
};
