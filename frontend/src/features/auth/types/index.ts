export interface User {
  id: number;
  email: string;
  first_name: string;
  last_name: string;
  role: "CUSTOMER" | "TECHNICIAN" | "ADMIN";
  verified: boolean;
}

export interface LoginCredentials {
  email: string;
  password: string;
}

export interface RegisterInput {
  email: string;
  password: string;
  first_name: string;
  last_name: string;
  phone?: string;
}

export interface VerifyOtpInput {
  email: string;
  code: string;
}

export interface AuthResponseData {
  token: string;
  user: User;
}
