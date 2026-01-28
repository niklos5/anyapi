const AUTH_URL =
  process.env.NEXT_PUBLIC_AUTH_URL ?? "http://localhost:9000";

const TOKEN_KEY = "anyapi_access_token";

export const getAccessToken = (): string | null => {
  if (typeof window === "undefined") {
    return null;
  }
  return window.localStorage.getItem(TOKEN_KEY);
};

export const setAccessToken = (token: string) => {
  if (typeof window === "undefined") {
    return;
  }
  window.localStorage.setItem(TOKEN_KEY, token);
};

export const clearAccessToken = () => {
  if (typeof window === "undefined") {
    return;
  }
  window.localStorage.removeItem(TOKEN_KEY);
};

export const login = async (email: string, password: string) => {
  const response = await fetch(`${AUTH_URL}/login`, {
    method: "POST",
    credentials: "include",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email, password }),
  });
  if (!response.ok) {
    throw new Error("Login failed");
  }
  const data = await response.json();
  if (data.access_token) {
    setAccessToken(data.access_token);
  }
  return data;
};

export const refresh = async () => {
  const response = await fetch(`${AUTH_URL}/refresh`, {
    method: "POST",
    credentials: "include",
  });
  if (!response.ok) {
    throw new Error("Refresh failed");
  }
  const data = await response.json();
  if (data.access_token) {
    setAccessToken(data.access_token);
  }
  return data;
};

export const logout = async () => {
  await fetch(`${AUTH_URL}/logout`, {
    method: "POST",
    credentials: "include",
  });
  clearAccessToken();
};
