# Methodologie

## Dataset
Het experiment is uitgevoerd op:
- een handmatig beoordeelde steekproef van 300 monumentenomschrijvingen;
- een grotere set van circa 64.000 monumenten (alleen voor verkenning).

## Pipeline
1. Inlezen van omschrijvingen uit CSV.
2. Classificatie via vaste AI-prompt.
3. Verplichte JSON-output.
4. Validatie van schema en labels.
5. Opslag van resultaten voor analyse.

## Promptstrategie
De AI is gebruikt als regelvolgende classifier.
Creativiteit is expliciet uitgeschakeld (temperature = 0).
Alle definities en prioriteitsregels zijn vastgelegd.

Naast het eindlabel per omschrijving is aanvullend een analyse op signaalniveau uitgevoerd.
Hierbij is gekeken of bepaalde formuleringstypen ergens in de omschrijving voorkomen,
ongeacht prioriteit. Deze analyse is uitsluitend gebruikt voor evaluatie en interpretatie,
niet voor classificatie of beleidsdoorgeleiding.


## Reproduceerbaarheid
- Vaste prompts.
- Vaste drempelwaarden.
- Geen interactieve correcties.
- Alle stappen zijn scriptmatig herhaalbaar.
