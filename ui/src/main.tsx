import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter } from "react-router";
import { FrameworkProvider } from "@sunbeam/g2v/providers";
import { createKanbanTransport } from "./auth/transport";
import { App } from "./App";
import "./styles.css";

const transport = createKanbanTransport();

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <FrameworkProvider transport={transport}>
      <BrowserRouter>
        <App />
      </BrowserRouter>
    </FrameworkProvider>
  </React.StrictMode>,
);
