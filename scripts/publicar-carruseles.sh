#!/usr/bin/env bash
# Publicar 2 carruseles reales (child_attachments) en Stellantis_Core + asociar a ads
# Uso:  TOKEN="EAA..." bash scripts/publicar-carruseles.sh
set -euo pipefail
TOKEN="${TOKEN:?Necesito TOKEN env var con tu access token Meta (ads_management scope)}"
ACCT="act_1434165342081795"
PAGE="109058168830247"
IG="17841461892533415"
ADSET="120251055962940779"      # Aon_Broad_Gdl
BASE="https://graph.facebook.com/v24.0"
LINK="https://stellantis.grupoplasencia.com/?utm_source=meta&utm_medium=cpc&utm_campaign=core_evergreen"
CAPTION="stellantis.grupoplasencia.com"
MSG1="13 modelos siempre disponibles en Plasencia. Enganche desde \$66,200* o bono lealtad Jeep. Tu asesor construye tu oferta a la medida con tasa STM Financial. Te contactamos en menos de 2h por WhatsApp. Sin compromiso ni costo."
MSG2="Lineup Stellantis Plasencia evergreen. 13 favoritos con enganche desde \$66,200* o bono de lealtad Wrangler. Tu asesor confirma tasa, mensualidad y bonos vigentes. Sin compromiso ni costo. Te contactamos en <2h por WhatsApp."

echo "================== CARRUSEL 1: Catálogo SPA (10 cards swipe) =================="
CARR1=$(curl -s -X POST "$BASE/$ACCT/adcreatives" \
  -F "access_token=$TOKEN" \
  -F "name=STL_Core_Carrusel_Catalogo_10cards_SPA" \
  -F "object_story_spec={\"page_id\":\"$PAGE\",\"instagram_user_id\":\"$IG\",\"link_data\":{\"link\":\"$LINK&utm_content=carr1_catalogo\",\"message\":\"$MSG1\",\"caption\":\"$CAPTION\",\"call_to_action\":{\"type\":\"SIGN_UP\",\"value\":{\"link\":\"$LINK&utm_content=carr1_catalogo\"}},\"multi_share_optimized\":true,\"multi_share_end_card\":false,\"child_attachments\":[
    {\"image_hash\":\"330235ee56de1bb1b9e8de265cefefec\",\"name\":\"RAM 700\",\"description\":\"Enganche desde \$66,200*\",\"link\":\"$LINK&utm_content=carr1_ram700\",\"call_to_action\":{\"type\":\"SIGN_UP\",\"value\":{\"link\":\"$LINK&utm_content=carr1_ram700\"}}},
    {\"image_hash\":\"8869f4f24ddaf0c27a64790c9cc68afb\",\"name\":\"Fiat Pulse\",\"description\":\"Enganche desde \$70,000*\",\"link\":\"$LINK&utm_content=carr1_pulse\",\"call_to_action\":{\"type\":\"SIGN_UP\",\"value\":{\"link\":\"$LINK&utm_content=carr1_pulse\"}}},
    {\"image_hash\":\"ae3a940557b9429a0f6d1e1deef3f8cb\",\"name\":\"Dodge Attitude 2026\",\"description\":\"Enganche desde \$79,800*\",\"link\":\"$LINK&utm_content=carr1_attitude\",\"call_to_action\":{\"type\":\"SIGN_UP\",\"value\":{\"link\":\"$LINK&utm_content=carr1_attitude\"}}},
    {\"image_hash\":\"348744cd27a9b03076c0a297b1514f36\",\"name\":\"RAM 1200\",\"description\":\"Enganche desde \$86,200*\",\"link\":\"$LINK&utm_content=carr1_ram1200\",\"call_to_action\":{\"type\":\"SIGN_UP\",\"value\":{\"link\":\"$LINK&utm_content=carr1_ram1200\"}}},
    {\"image_hash\":\"95e8188c7408c0700e47582103ddf0cf\",\"name\":\"Jeep Renegade 2026\",\"description\":\"Enganche desde \$93,300*\",\"link\":\"$LINK&utm_content=carr1_renegade\",\"call_to_action\":{\"type\":\"SIGN_UP\",\"value\":{\"link\":\"$LINK&utm_content=carr1_renegade\"}}},
    {\"image_hash\":\"1d28ce27a1eced55351387ec4624e7e1\",\"name\":\"Fiat Fastback 2026\",\"description\":\"Enganche desde \$94,500*\",\"link\":\"$LINK&utm_content=carr1_fastback\",\"call_to_action\":{\"type\":\"SIGN_UP\",\"value\":{\"link\":\"$LINK&utm_content=carr1_fastback\"}}},
    {\"image_hash\":\"3ebdfa8af14de61f2216222585576de5\",\"name\":\"Peugeot 2008 2026\",\"description\":\"Enganche desde \$96,000*\",\"link\":\"$LINK&utm_content=carr1_2008\",\"call_to_action\":{\"type\":\"SIGN_UP\",\"value\":{\"link\":\"$LINK&utm_content=carr1_2008\"}}},
    {\"image_hash\":\"8e5bd138bb7c4cc89613cbd7448d8a6e\",\"name\":\"Jeep Compass\",\"description\":\"Enganche desde \$111,200*\",\"link\":\"$LINK&utm_content=carr1_compass\",\"call_to_action\":{\"type\":\"SIGN_UP\",\"value\":{\"link\":\"$LINK&utm_content=carr1_compass\"}}},
    {\"image_hash\":\"d3965e8cd9b12909037b400276106d55\",\"name\":\"Peugeot 3008 2026\",\"description\":\"Enganche desde \$122,700*\",\"link\":\"$LINK&utm_content=carr1_3008\",\"call_to_action\":{\"type\":\"SIGN_UP\",\"value\":{\"link\":\"$LINK&utm_content=carr1_3008\"}}},
    {\"image_hash\":\"17ee6088b4b6764e0147e616a5df4fe6\",\"name\":\"Jeep Wrangler\",\"description\":\"Bono lealtad \$30,000 + MSI\",\"link\":\"$LINK&utm_content=carr1_wrangler\",\"call_to_action\":{\"type\":\"SIGN_UP\",\"value\":{\"link\":\"$LINK&utm_content=carr1_wrangler\"}}}
  ]}}")
CARR1_ID=$(echo "$CARR1" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("id",""))')
echo "Creative carrusel 1: $CARR1_ID"
echo "$CARR1" | python3 -m json.tool | head -20

echo ""
echo "================== CARRUSEL 2: Assets Bere evergreen (10 cards swipe) =================="
CARR2=$(curl -s -X POST "$BASE/$ACCT/adcreatives" \
  -F "access_token=$TOKEN" \
  -F "name=STL_Core_Carrusel_Bere_Evergreen_10cards" \
  -F "object_story_spec={\"page_id\":\"$PAGE\",\"instagram_user_id\":\"$IG\",\"link_data\":{\"link\":\"$LINK&utm_content=carr2_bere\",\"message\":\"$MSG2\",\"caption\":\"$CAPTION\",\"call_to_action\":{\"type\":\"SIGN_UP\",\"value\":{\"link\":\"$LINK&utm_content=carr2_bere\"}},\"multi_share_optimized\":true,\"multi_share_end_card\":false,\"child_attachments\":[
    {\"image_hash\":\"87b9041047e7572943c3125235f078e4\",\"name\":\"RAM 700\",\"description\":\"Enganche desde \$66,200*\",\"link\":\"$LINK&utm_content=carr2_ram700\",\"call_to_action\":{\"type\":\"SIGN_UP\",\"value\":{\"link\":\"$LINK&utm_content=carr2_ram700\"}}},
    {\"image_hash\":\"7c61871b6f2ad4df6bc9db938db2b5e3\",\"name\":\"Fiat Pulse\",\"description\":\"Enganche desde \$70,000*\",\"link\":\"$LINK&utm_content=carr2_pulse\",\"call_to_action\":{\"type\":\"SIGN_UP\",\"value\":{\"link\":\"$LINK&utm_content=carr2_pulse\"}}},
    {\"image_hash\":\"4875d2d04fb968ede383a43bffedfc84\",\"name\":\"Dodge Attitude 2026\",\"description\":\"Enganche desde \$79,800*\",\"link\":\"$LINK&utm_content=carr2_attitude\",\"call_to_action\":{\"type\":\"SIGN_UP\",\"value\":{\"link\":\"$LINK&utm_content=carr2_attitude\"}}},
    {\"image_hash\":\"d92f57e00312239b4f2e690ffda7b3f6\",\"name\":\"RAM 1200\",\"description\":\"Enganche desde \$86,200*\",\"link\":\"$LINK&utm_content=carr2_ram1200\",\"call_to_action\":{\"type\":\"SIGN_UP\",\"value\":{\"link\":\"$LINK&utm_content=carr2_ram1200\"}}},
    {\"image_hash\":\"8649cb9185213ab85abd41ebfc0adc03\",\"name\":\"Jeep Renegade 2026\",\"description\":\"Enganche desde \$93,300*\",\"link\":\"$LINK&utm_content=carr2_renegade\",\"call_to_action\":{\"type\":\"SIGN_UP\",\"value\":{\"link\":\"$LINK&utm_content=carr2_renegade\"}}},
    {\"image_hash\":\"edbdc1c6d4b0bbcd2a07f3c1ca4c9394\",\"name\":\"Fiat Fastback\",\"description\":\"Enganche desde \$94,500*\",\"link\":\"$LINK&utm_content=carr2_fastback\",\"call_to_action\":{\"type\":\"SIGN_UP\",\"value\":{\"link\":\"$LINK&utm_content=carr2_fastback\"}}},
    {\"image_hash\":\"8aeaf3ce34ab8555ee023b3da87cd8fc\",\"name\":\"Peugeot 2008 2026\",\"description\":\"Enganche desde \$96,000*\",\"link\":\"$LINK&utm_content=carr2_2008\",\"call_to_action\":{\"type\":\"SIGN_UP\",\"value\":{\"link\":\"$LINK&utm_content=carr2_2008\"}}},
    {\"image_hash\":\"ce8745aa04f84c8d72ab2cf641964a76\",\"name\":\"Jeep Compass\",\"description\":\"Enganche desde \$111,200*\",\"link\":\"$LINK&utm_content=carr2_compass\",\"call_to_action\":{\"type\":\"SIGN_UP\",\"value\":{\"link\":\"$LINK&utm_content=carr2_compass\"}}},
    {\"image_hash\":\"762f38f4e975c3eedd1f50f713d43ef8\",\"name\":\"Peugeot 3008 2026\",\"description\":\"Enganche desde \$122,700*\",\"link\":\"$LINK&utm_content=carr2_3008\",\"call_to_action\":{\"type\":\"SIGN_UP\",\"value\":{\"link\":\"$LINK&utm_content=carr2_3008\"}}},
    {\"image_hash\":\"20d2937214ea06efec96e12c10872edf\",\"name\":\"Jeep Wrangler\",\"description\":\"Bono lealtad \$30,000 + MSI\",\"link\":\"$LINK&utm_content=carr2_wrangler\",\"call_to_action\":{\"type\":\"SIGN_UP\",\"value\":{\"link\":\"$LINK&utm_content=carr2_wrangler\"}}}
  ]}}")
CARR2_ID=$(echo "$CARR2" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("id",""))')
echo "Creative carrusel 2: $CARR2_ID"
echo "$CARR2" | python3 -m json.tool | head -20

echo ""
echo "================== Pausar 2 ads de menor performance =================="
# 5008_story ($211 spend) y PEUGEOT_2008 ($157 spend) — los 2 de menor inversión
for AD in 120252164431880779 120252164461550779; do
  curl -s -X POST "$BASE/$AD" -F "access_token=$TOKEN" -F "status=PAUSED" | python3 -m json.tool
done

echo ""
echo "================== Crear 2 ads nuevos en adset Core =================="
for ENTRY in "carr1:$CARR1_ID:STL_Carrusel1_Catalogo_SPA" "carr2:$CARR2_ID:STL_Carrusel2_Bere_Evergreen"; do
  IFS=':' read -r TAG CID NAME <<< "$ENTRY"
  echo "→ $NAME (creative $CID)"
  curl -s -X POST "$BASE/$ACCT/ads" \
    -F "access_token=$TOKEN" \
    -F "name=$NAME" \
    -F "adset_id=$ADSET" \
    -F "creative={\"creative_id\":\"$CID\"}" \
    -F "status=ACTIVE" | python3 -m json.tool
done

echo ""
echo "================== DONE =================="
echo "Carruseles publicados en Stellantis_Core_StellantisPlasencia. Revisa en Ads Manager."
