const AUTH_URL =
  process.env.NEXT_PUBLIC_AUTH_URL ?? "http://localhost:9000";

const TOKEN_KEY = "anyapi_access_token";
let accessToken: string | null = null;
let refreshInFlight: Promise<string | null> | null = null;

export const getAccessToken = (): string | null => {
  if (typeof window === "undefined") {
    return null;
  }
  if (accessToken) {
    return accessToken;
  }
  accessToken = window.localStorage.getItem(TOKEN_KEY);
  return accessToken;
};

export const setAccessToken = (token: string) => {
  if (typeof window === "undefined") {
    return;
  }
  accessToken = token;
  window.localStorage.setItem(TOKEN_KEY, token);
};

export const clearAccessToken = () => {
  if (typeof window === "undefined") {
    return;
  }
  accessToken = null;
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

export const signup = async (payload: {
  company: string;
  email: string;
  password: string;
}) => {
  const response = await fetch(`${AUTH_URL}/signup`, {
    method: "POST",
    credentials: "include",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    throw new Error("Signup failed");
  }
  const data = await response.json();
  if (data.access_token) {
    setAccessToken(data.access_token);
  }
  return data;
};

export const refresh = async () => {
  if (refreshInFlight) {
    return refreshInFlight;
  }
  refreshInFlight = (async () => {
    const response = await fetch(`${AUTH_URL}/refresh`, {
      method: "POST",
      credentials: "include",
    });
    if (!response.ok) {
      throw new Error("Refresh failed");
    }
    const data = await response.json();
    const token = data.access_token ?? null;
    if (token) {
      setAccessToken(token);
    } else {
      clearAccessToken();
    }
    return token;
  })();

  try {
    return await refreshInFlight;
  } finally {
    refreshInFlight = null;
  }
};

export const logout = async () => {
  await fetch(`${AUTH_URL}/logout`, {
    method: "POST",
    credentials: "include",
  });
  clearAccessToken();
};

export const bootstrapSession = async () => {
  try {
    await refresh();
  } catch {
    clearAccessToken();
  }
  return getAccessToken();
};
