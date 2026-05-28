import { create } from 'zustand'

interface AuthState {
  token: string | null;
  user: { username: string; roles: string[] } | null;
  setToken: (token: string) => void;
  logout: () => void;
}

interface IncidentState {
  activeIncidents: any[];
  addIncident: (incident: any) => void;
  clearIncidents: () => void;
}

export const useAuthStore = create<AuthState>((set) => ({
  token: null,
  user: null,
  setToken: (token) => {
    // In a real app, you'd decode the JWT here to set the user
    set({ token, user: { username: 'admin', roles: ['admin'] } });
  },
  logout: () => set({ token: null, user: null }),
}))

export const useIncidentStore = create<IncidentState>((set) => ({
  activeIncidents: [],
  addIncident: (incident) => set((state) => ({ 
    activeIncidents: [incident, ...state.activeIncidents].slice(0, 50) 
  })),
  clearIncidents: () => set({ activeIncidents: [] }),
}))
