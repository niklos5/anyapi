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

export type TransformerInputConfig = {
  sourceType: DataSourceType;
  endpoint?: string;
  apiKey?: string;
  cloudPath?: string;
};

export type TransformerOutputConfig = {
  destinationType: "webhook" | "s3" | "db";
  webhookUrl?: string;
  s3Path?: string;
  connectionString?: string;
};

export type TransformerSettings = {
  dedupeEnabled: boolean;
};

export type TransformerMetadata = {
  input?: TransformerInputConfig;
  output?: TransformerOutputConfig;
  settings?: TransformerSettings;
};
