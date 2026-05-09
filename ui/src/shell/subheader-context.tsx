import { createContext, useContext, useState, type ReactNode } from "react";

interface SubheaderContextValue {
  content: ReactNode;
  setContent: (content: ReactNode) => void;
}

const SubheaderContext = createContext<SubheaderContextValue>({
  content: null,
  setContent: () => {},
});

export function SubheaderProvider({ children }: { children: ReactNode }) {
  const [content, setContent] = useState<ReactNode>(null);
  return (
    <SubheaderContext.Provider value={{ content, setContent }}>
      {children}
    </SubheaderContext.Provider>
  );
}

export function useSubheader() {
  return useContext(SubheaderContext);
}
