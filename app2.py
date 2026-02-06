import streamlit as st
import pymongo
import pandas as pd
import plotly.express as px
import re

# --- CONFIGURATION ---
st.set_page_config(page_title="World Cup Tweets Dashboard", layout="wide")

# Utilisation de ton URI Atlas
ATLAS_URI = "mongodb+srv://mspar02:mspar02@testcluster-mongodb.xvzubbe.mongodb.net/?appName=TestCluster-MongoDB"

@st.cache_resource
def init_connection():
    return pymongo.MongoClient(ATLAS_URI)

client = init_connection()

# --- RÉCUPÉRATION DES DONNÉES ---
@st.cache_data(ttl=600)
def load_data():
    db = client["twitter_db"]
    collection = db["worldcup_tweets"]
    # On récupère tous les champs extraits par ton ETL
    cursor = collection.find({}, {
        "_id": 0, 
        "user_handle": 1, 
        "text": 1, 
        "lang": 1, 
        "retweet_count": 1, 
        "date_only": 1, 
        "hashtags": 1, 
        "source": 1,
        "user_location": 1,
        "is_retweet_id": 1
    })
    return pd.DataFrame(list(cursor))

st.title("⚽ Dashboard World Cup 2026 - Data Analysis")
st.markdown("Exploration des données issues de l'ETL PySpark stockées sur **MongoDB Atlas**.")

with st.spinner("Récupération des tweets depuis le cloud..."):
    df = load_data()

if df.empty:
    st.error("⚠️ Aucune donnée trouvée sur Atlas. Vérifie que ton ETL a bien fonctionné.")
else:
    # --- BARRE LATÉRALE ---
    st.sidebar.header("🔍 Filtres globaux")
    
    # Filtre par langue
    langs = sorted(df['lang'].dropna().unique())
    lang_choice = st.sidebar.multiselect("Langues", langs, default=langs)
    
    # Filtre Tweets originaux vs Retweets
    show_retweets = st.sidebar.checkbox("Inclure les Retweets", value=True)
    
    # Application des filtres
    df_filtered = df[df['lang'].isin(lang_choice)]
    if not show_retweets:
        df_filtered = df_filtered[df_filtered['is_retweet_id'].isna()]

    # --- KPI GÉNÉRAUX ---
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Tweets", f"{len(df_filtered):,}")
    col2.metric("Utilisateurs", f"{df_filtered['user_handle'].nunique():,}")
    col3.metric("RT Cumulés", f"{int(df_filtered['retweet_count'].sum()):,}")
    col4.metric("Villes citées", f"{df_filtered['user_location'].dropna().nunique():,}")

    st.divider()

    # --- VISUALISATIONS TEMPORY ET LANGUE ---
    row1_col1, row1_col2 = st.columns(2)

    with row1_col1:
        st.subheader("📈 Évolution temporelle")
        df_counts = df_filtered.groupby('date_only').size().reset_index(name='Nombre')
        df_counts = df_counts.sort_values('date_only')
        fig_line = px.line(df_counts, x='date_only', y='Nombre', markers=True, template="plotly_dark", color_discrete_sequence=['#00CC96'])
        st.plotly_chart(fig_line, use_container_width=True)

    with row1_col2:
        st.subheader("🌍 Répartition par langue")
        fig_pie = px.pie(df_filtered, names='lang', hole=0.4, color_discrete_sequence=px.colors.qualitative.Pastel)
        st.plotly_chart(fig_pie, use_container_width=True)

    st.divider()

    # --- ANALYSE DES HASHTAGS (POURCENTAGE) ---
    st.subheader("📊 Top Hashtags (Part de voix en %)")
    
    # Sécurité : on filtre les tweets sans hashtags
    df_has_tags = df_filtered.dropna(subset=['hashtags'])
    if not df_has_tags.empty:
        all_tags = df_has_tags.explode('hashtags')
        tag_counts = all_tags['hashtags'].value_counts().head(15).reset_index()
        tag_counts.columns = ['Hashtag', 'Occurrences']
        tag_counts['Pourcentage'] = (tag_counts['Occurrences'] / len(df_filtered)) * 100
        
        fig_tags = px.bar(tag_counts, x='Pourcentage', y='Hashtag', orientation='h', 
                          color='Pourcentage', color_continuous_scale='Viridis',
                          text=tag_counts['Pourcentage'].apply(lambda x: f'{x:.1f}%'))
        fig_tags.update_traces(textposition='outside')
        st.plotly_chart(fig_tags, use_container_width=True)
    else:
        st.info("Aucun hashtag détecté dans cette sélection.")

    # --- ANALYSE DES MOTS-CLÉS ---
    st.divider()
    st.subheader("🔤 Mots les plus fréquents (Hors Hashtags)")
    
    STOP_WORDS = set([
        "have", "your", "here", "will", "the", "to", "and", "is", "in", "it", "you", "of", "for", "on", "my", "at", "with", "rt", "http", "https", "co", "a", "an", "de", "que", "da", "em", "um", "do", "this", "that", "from"
    ])
    
    # Nettoyage rapide du texte
    texts = " ".join(df_filtered['text'].astype(str)).lower()
    words = re.findall(r'\w+', texts)
    cleaned_words = [w for w in words if len(w) > 3 and w not in STOP_WORDS and not w.isdigit()]
    
    if cleaned_words:
        df_w = pd.DataFrame(cleaned_words, columns=['Mot']).value_counts().head(20).reset_index()
        df_w.columns = ['Mot', 'Fréquence']
        fig_words = px.bar(df_w, x='Fréquence', y='Mot', orientation='h', color='Fréquence', color_continuous_scale='Reds')
        st.plotly_chart(fig_words, use_container_width=True)

    # --- SOURCES ET APPAREILS ---
    st.divider()
    st.subheader("📱 Appareils et Sources")
    
    if 'source' in df_filtered.columns:
        def clean_source(x):
            if pd.isna(x): return "Unknown"
            res = re.search(r'>(.*?)</a>', str(x))
            return res.group(1) if res else str(x)
        
        df_filtered['source_clean'] = df_filtered['source'].apply(clean_source)
        src_counts = df_filtered['source_clean'].value_counts().head(8).reset_index()
        src_counts.columns = ['Source', 'Total']
        
        fig_src = px.pie(src_counts, names='Source', values='Total', hole=0.5, template="plotly_dark")
        st.plotly_chart(fig_src, use_container_width=True)

    
    # --- TWEETS LES PLUS RETWEETÉS ---
    st.divider()
    st.subheader("🔥 Top 10 des Tweets les plus retweetés")

    # On trie et on sélectionne les colonnes importantes
    top_rt = df_filtered.sort_values('retweet_count', ascending=False).head(10)

    if not top_rt.empty:
        # Création d'un conteneur pour styliser l'affichage
        for index, row in top_rt.iterrows():
            with st.expander(f"📢 {row['user_handle']} - {int(row['retweet_count']):,} Retweets"):
                st.write(f"**Texte :** {row['text']}")
                st.caption(f"📅 Date : {row['date_only']} | 🌍 Langue : {row['lang'].upper()} | 📱 Source : {row['source_clean'] if 'source_clean' in row else 'N/A'}")
                
        # Alternative : Affichage sous forme de graphique pour comparer l'impact
        st.write("#### 📊 Comparaison de l'impact (Retweets)")
        fig_top_rt = px.bar(
            top_rt,
            x='retweet_count',
            y='user_handle',
            orientation='h',
            text='retweet_count',
            color='retweet_count',
            color_continuous_scale='Reds',
            labels={'retweet_count': 'Nombre de Retweets', 'user_handle': 'Utilisateur'}
        )
        fig_top_rt.update_layout(yaxis={'categoryorder':'total ascending'}, template="plotly_dark")
        st.plotly_chart(fig_top_rt, use_container_width=True)
    else:
        st.write("Aucun tweet trouvé.")

    
    # --- ANALYSE DE LA LOCALISATION ---
    st.divider()
    st.subheader("📍 Top 10 des Localisations déclarées")

    # On nettoie les valeurs nulles et on compte
    # On filtre les lieux trop génériques si nécessaire (ex: "Earth", "Worldwide")
    df_loc = df_filtered.dropna(subset=['user_location'])
    
    if not df_loc.empty:
        top_locations = df_loc['user_location'].value_counts().head(10).reset_index()
        top_locations.columns = ['Localisation', 'Nombre de Tweets']

        col_loc_chart, col_loc_list = st.columns([2, 1])

        with col_loc_chart:
            # Graphique à barres pour les localisations
            fig_loc = px.bar(
                top_locations,
                x='Nombre de Tweets',
                y='Localisation',
                orientation='h',
                color='Nombre de Tweets',
                color_continuous_scale='GnBu',
                title="Villes / Régions les plus actives",
                template="plotly_dark"
            )
            # Tri pour avoir la plus grande barre en haut
            fig_loc.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig_loc, use_container_width=True)

        with col_loc_list:
            st.write("#### 📑 Classement")
            # Affichage d'un tableau propre
            st.dataframe(top_locations, hide_index=True)
            
        # --- PETITE ANALYSE POUR LE PROJET ---
        st.info(f"💡 La localisation **'{top_locations.iloc[0]['Localisation']}'** est la plus représentée. Est-ce une ville hôte ?")
    else:
        st.warning("Aucune donnée de localisation disponible dans cette sélection.")