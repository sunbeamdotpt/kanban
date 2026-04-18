import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import { useTheme } from "@sunbeam/beam-ui";
import AppLayout from "./layouts/AppLayout";
import Projects from "./pages/Projects";
import BoardPage from "./pages/Board";

export default function App() {
  useTheme();

  return (
    <BrowserRouter>
      <Routes>
        <Route element={<AppLayout />}>
          <Route path="/" element={<Projects />} />
          <Route path="/p/:projectSlug/b/:boardSlug" element={<BoardPage />} />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}
