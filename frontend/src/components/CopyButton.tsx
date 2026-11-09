import { useEffect, useMemo, useRef, useState } from "react";
import { Check, Copy, LoaderCircle } from "lucide-react";
import {
  clipboardBytes,
  MAX_CLIPBOARD_BYTES,
  writeClipboard,
} from "../clipboard";
import "./CopyButton.css";

export default function CopyButton({
  text,
  label = "Copier les données du graphique",
  caption,
  disabled = false,
}: {
  text: string;
  label?: string;
  caption?: string;
  disabled?: boolean;
}) {
  const [state, setState] = useState<"idle" | "busy" | "copied" | "error">("idle");
  const [error, setError] = useState("");
  const generation = useRef(0);
  const busy = useRef(false);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const tooLarge = useMemo(
    () => clipboardBytes(text) > MAX_CLIPBOARD_BYTES,
    [text],
  );
  useEffect(() => {
    generation.current += 1;
    busy.current = false;
    setState("idle");
    setError("");
    return () => {
      generation.current += 1;
      if (timer.current) clearTimeout(timer.current);
    };
  }, [text]);

  const copy = async () => {
    if (busy.current || disabled || tooLarge || !text) return;
    busy.current = true;
    if (timer.current) clearTimeout(timer.current);
    const current = generation.current;
    setState("busy");
    setError("");
    try {
      // Call during the click, without fetching first: Firefox/Safari require
      // user activation. Announce success only after the actual write resolves.
      await writeClipboard(text);
      if (current !== generation.current) return;
      setState("copied");
      timer.current = setTimeout(() => setState("idle"), 2000);
    } catch (cause) {
      if (current !== generation.current) return;
      setState("error");
      setError(
        cause instanceof Error
          ? `Copie impossible : ${cause.message}`
          : "Copie impossible. Vérifiez l’autorisation du presse-papiers.",
      );
    } finally {
      if (current === generation.current) busy.current = false;
    }
  };

  return (
    <span className="copy-control">
      <button
        type="button"
        className={caption ? "button button-secondary" : "icon-button"}
        aria-label={label}
        title={tooLarge ? "Copie trop volumineuse (plus de 5 Mio)" : label}
        disabled={disabled || tooLarge || !text || state === "busy"}
        onClick={() => void copy()}
      >
        {state === "busy" ? (
          <LoaderCircle size={16} className="spin" aria-hidden="true" />
        ) : state === "copied" ? (
          <Check size={16} aria-hidden="true" />
        ) : (
          <Copy size={16} aria-hidden="true" />
        )}
        {caption}
      </button>
      <span className="copy-feedback" role="status" aria-live="polite">
        {state === "copied" ? "Copié" : ""}
      </span>
      {state === "error" && (
        <span className="copy-error" role="alert">
          {error}
        </span>
      )}
    </span>
  );
}
