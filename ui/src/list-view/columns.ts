/**
 * columns.ts
 *
 * Column definitions for the ListView table.
 * Columns: Ref, Title, Column, Assignees, Labels, Priority, Due, Status
 */

/** Column configuration for ListView table (mirrors beam-ui Table Column interface). */
export interface TableColumn {
  key: string;
  label: string;
  sortable?: boolean;
  width?: string;
}

export const listViewColumns: TableColumn[] = [
  { key: "ref", label: "Ref", sortable: true, width: "80px" },
  { key: "title", label: "Title", sortable: true },
  { key: "columnName", label: "Column", sortable: true, width: "120px" },
  { key: "assignees", label: "Assignees", sortable: false, width: "120px" },
  { key: "labels", label: "Labels", sortable: false, width: "150px" },
  { key: "priority", label: "Priority", sortable: true, width: "100px" },
  { key: "due", label: "Due", sortable: true, width: "100px" },
  { key: "status", label: "Status", sortable: true, width: "100px" },
];
