import { useEffect, useRef, type ReactNode } from "react";
interface ModalProps {
  open: boolean;
  onClose: () => void;
  className: string;
  label?: string;
  labelledBy?: string;
  children: ReactNode;
}
export function Modal({
  open,
  onClose,
  className,
  label,
  labelledBy,
  children,
}: ModalProps) {
  const ref = useRef<HTMLDialogElement>(null);
  useEffect(() => {
    const dialog = ref.current;
    if (!open || !dialog) return;
    dialog.showModal();
    return () => dialog.close();
  }, [open]);
  return (
    <dialog
      ref={ref}
      className={className}
      aria-label={label}
      aria-labelledby={labelledBy}
      onCancel={onClose}
      onClick={(event) => {
        if (event.target === event.currentTarget) onClose();
      }}
    >
      {children}
    </dialog>
  );
}
