import { Routes, Route, Navigate } from "react-router-dom";

import { useAuth } from "./context/AuthContext";

import FormPage from "./pages/FormPage";
import LoginPage from "./pages/LoginPage";
import RegisterPage from "./pages/RegisterPage";

export default function App() {
  const { isAuthenticated } = useAuth();

  return (
    <Routes>
      {/* Protected Route */}
      <Route
        path="/"
        element={
          isAuthenticated ? <FormPage /> : <Navigate to="/login" replace />
        }
      />

      {/* Public Routes */}
      <Route
        path="/login"
        element={!isAuthenticated ? <LoginPage /> : <Navigate to="/" replace />}
      />

      <Route
        path="/register"
        element={
          !isAuthenticated ? <RegisterPage /> : <Navigate to="/" replace />
        }
      />

      {/* Catch all routes */}
      <Route
        path="*"
        element={<Navigate to={isAuthenticated ? "/" : "/login"} replace />}
      />
    </Routes>
  );
}
