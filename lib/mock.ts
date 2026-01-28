import { IngestionJob } from "@/lib/types";

export const mockJobs: IngestionJob[] = [
  {
    id: "job_3812",
    name: "Shopify Orders - April",
    sourceType: "api",
    createdAt: "2026-01-26 09:14",
    status: "completed",
    records: 1823,
    issues: [
      {
        field: "customer_phone",
        level: "warning",
        message: "42 records missing phone number.",
      },
    ],
  },
  {
    id: "job_3813",
    name: "Warehouse Inventory",
    sourceType: "file",
    createdAt: "2026-01-27 14:02",
    status: "processing",
    records: 9021,
    issues: [],
  },
  {
    id: "job_3814",
    name: "Marketing Leads",
    sourceType: "cloud",
    createdAt: "2026-01-27 18:45",
    status: "failed",
    records: 458,
    issues: [
      {
        field: "lead_score",
        level: "error",
        message: "Field contains mixed types (string, number).",
      },
      {
        field: "region",
        level: "error",
        message: "Unknown region values in 12 rows.",
      },
    ],
  },
];

export const mockPreviewRows = [
  {
    external_id: "ORD-7781",
    customer_name: "Maya Patel",
    customer_email: "maya@example.com",
    order_total: 229.5,
    order_date: "2026-01-25",
  },
  {
    external_id: "ORD-7782",
    customer_name: "Darius Smith",
    customer_email: "darius@example.com",
    order_total: 115.0,
    order_date: "2026-01-25",
  },
  {
    external_id: "ORD-7783",
    customer_name: "Priya Desai",
    customer_email: "priya@example.com",
    order_total: 89.99,
    order_date: "2026-01-26",
  },
];
