import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import CopyButton from "./components/CopyButton";
import { PageHeader } from "./components/ui";
import { MAX_CLIPBOARD_BYTES } from "./clipboard";

const sources = import.meta.glob<string>(
  ["./pages/*.tsx", "./components/Layout.tsx"],
  { query: "?raw", import: "default", eager: true },
);
const source = (file: string) => {
  const text = sources[file];
  if (text === undefined) throw new Error(`Missing source: ${file}`);
  return text;
};

describe("copy UI and compact pages", () => {
  it("renders a labelled icon button and an initially empty live notification", () => {
    const markup = renderToStaticMarkup(<CopyButton text="Date,Montant" />);
    expect(markup).toContain('aria-label="Copier les données du graphique"');
    expect(markup).toContain('type="button"');
    expect(markup).toContain('role="status"');
    expect(markup).not.toContain("Copié");
  });
  it("disables empty and oversized copies rather than truncating them", () => {
    expect(renderToStaticMarkup(<CopyButton text="" />)).toContain("disabled");
    expect(
      renderToStaticMarkup(
        <CopyButton text={"x".repeat(MAX_CLIPBOARD_BYTES + 1)} />,
      ),
    ).toContain("disabled");
  });
  it("does not leave an empty subtitle paragraph", () => {
    const markup = renderToStaticMarkup(
      <PageHeader eyebrow="Test" title="Titre" />,
    );
    expect(markup).not.toContain("<p");
  });
  it("uses the shared vertical spacing between both allocation panels", () => {
    const page = source("./pages/Allocation.tsx");
    expect(page).toMatch(/<div className="stack">\s*<Panel/);
    expect(page).toContain('title="Détail de la situation"');
  });
  it("removes the requested prose and sidebar status dot", () => {
    const pages = [
      "Dashboard",
      "Transactions",
      "Flows",
      "Portfolio",
      "Allocation",
      "Fire",
      "Data",
    ];
    const text = pages.map((page) => source(`./pages/${page}.tsx`)).join(" ");
    for (const phrase of [
      "Votre argent du quotidien",
      "Dépenses regroupées selon",
      "Revenus, dépenses et tendance glissante",
      "Recherchez vos revenus et dépenses",
      "Les corrections se font",
      "Suivez le chemin de l’argent",
      "Une lecture consolidée",
      "Sorties à gauche",
      "Achats et ventes, des sources",
      "Filtrez, corrigez et exportez",
      "Valeur nette, historique des comptes",
      "Explorez le poids",
      "Survolez un bloc",
      "Hypothèses, événements de vie",
      "Import contrôlé, configuration",
    ]) {
      expect(text).not.toContain(phrase);
    }
    expect(source("./pages/Flows.tsx")).not.toContain(
      'title="Revenus → dépenses"',
    );
    const layout = source("./components/Layout.tsx");
    expect(layout).not.toContain("Données locales");
    expect(layout).not.toContain("status-dot");
  });
});
