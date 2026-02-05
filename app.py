import streamlit as st
import pymongo
import pandas as pd
import plotly.express as px

# --- CONFIGURATION ---
st.set_page_config(page_title="World Cup Tweets Dashboard", layout="wide")

# Remplace par ton URI Atlas réelle
ATLAS_URI = "mongodb+srv://mspar02:mspar02@testcluster-mongodb.xvzubbe.mongodb.net/?appName=TestCluster-MongoDB"

@st.cache_resource
def init_connection():
    return pymongo.MongoClient(ATLAS_URI)

client = init_connection()

# --- RÉCUPÉRATION DES DONNÉES ---
@st.cache_data(ttl=600) # Garde les données en cache 10 min
def load_data():
    db = client["twitter_db"]
    collection = db["worldcup_tweets"]
    # On récupère tout (ou on peut filtrer ici)
    data = list(collection.find({}, {"_id": 0, "user_handle": 1, "text": 1, "lang": 1, "retweet_count": 1, "date_only": 1, "hashtags": 1}))
    return pd.DataFrame(data)

st.title("⚽ Analyse des Tweets - World Cup 2026")
st.markdown("Ce dashboard interroge **MongoDB Atlas** pour explorer les sujets potentiels du projet.")

with st.spinner("Chargement des données depuis Atlas..."):
    df = load_data()

if df.empty:
    st.warning("La base de données est vide. Lance ton ETL d'abord !")
else:
    # --- BARRE LATÉRALE (Filtres) ---
    st.sidebar.header("Filtres")
    lang_choice = st.sidebar.multiselect("Choisir les langues", df['lang'].unique(), default=df['lang'].unique())
    df_filtered = df[df['lang'].isin(lang_choice)]

    # --- KPI GÉNÉRAUX ---
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Tweets", len(df_filtered))
    col2.metric("Utilisateurs uniques", df_filtered['user_handle'].nunique())
    col3.metric("Langue dominante", df_filtered['lang'].mode()[0].upper())

    st.divider()

    # --- VISUALISATIONS ---
    row1_col1, row1_col2 = st.columns(2)

    with row1_col1:
        st.subheader("📈 Volume de tweets par jour")
        # On s'assure que la date est bien triée
        df_counts = df_filtered.groupby('date_only').size().reset_index(name='counts')
        fig_line = px.line(df_counts, x='date_only', y='counts', markers=True, template="plotly_dark")
        st.plotly_chart(fig_line, use_container_width=True)

    with row1_col2:
        st.subheader("🌍 Répartition par langue")
        fig_pie = px.pie(df_filtered, names='lang', hole=0.4)
        st.plotly_chart(fig_pie, use_container_width=True)

    st.divider()

    # --- ANALYSE DES HASHTAGS ---
st.subheader("📊 Part de voix des Hashtags (en %)")

# 1. Calcul du nombre total de tweets uniques dans la sélection actuelle
total_tweets = len(df_filtered)

if total_tweets > 0:
    # 2. On "explose" la liste des hashtags
    all_hashtags = df_filtered.explode('hashtags')
    
    # 3. On compte les occurrences de chaque hashtag
    # Note : Cela compte dans combien de TWEETS chaque hashtag apparaît
    top_hashtags = all_hashtags['hashtags'].value_counts().head(15).reset_index()
    top_hashtags.columns = ['Hashtag', 'Occurrences']
    
    # 4. Calcul du pourcentage par rapport au total de tweets
    top_hashtags['Pourcentage'] = (top_hashtags['Occurrences'] / total_tweets) * 100

    # 5. Création du graphique avec Plotly
    fig_bar = px.bar(
        top_hashtags, 
        x='Pourcentage', 
        y='Hashtag', 
        orientation='h', 
        color='Pourcentage', 
        color_continuous_scale='Viridis',
        text=top_hashtags['Pourcentage'].apply(lambda x: f'{x:.1f}%'), # Affiche le % sur les barres
        title=f"Hashtags présents dans les {total_tweets} tweets analysés"
    )
    
    # Amélioration du rendu (texte à l'extérieur des barres)
    fig_bar.update_traces(textposition='outside')
    fig_bar.update_layout(xaxis_title="Pourcentage des tweets (%)", yaxis_title="Hashtags")
    
    st.plotly_chart(fig_bar, use_container_width=True)
else:
    st.write("Aucune donnée disponible pour calculer les hashtags.")

st.divider()
st.subheader("🔤 Analyse des mots-clés (hors Hashtags)")

    

# --- ANALYSE DES MOTS-CLÉS (hors hashtags et stop words) ---
STOP_WORDS = set([
    "the", "to", "and", "is", "in", "it", "you", "of", "for", "on", "my", "at", "with", "rt", "http", "https", "co", "a", "an", "de", "que", "da", "em", "um", "do", "this", "that"
])
# On récupère les textes, on les met en minuscule et on sépare les mots
all_text = " ".join(df_filtered['text'].astype(str)).lower()
# On enlève la ponctuation simple et on split
words = all_text.split()

# Filtrage : On garde les mots de plus de 3 lettres et qui ne sont pas des stop words ou des hashtags
cleaned_words = [
    word.strip(".,!?:;") for word in words 
    if len(word) > 3 
    and word not in STOP_WORDS 
    and not word.startswith('#') 
    and not word.startswith('@')
]

# Comptage et création du graphique
if cleaned_words:
    df_words = pd.DataFrame(cleaned_words, columns=['Mot']).value_counts().head(20).reset_index()
    df_words.columns = ['Mot', 'Fréquence']

    fig_words = px.bar(
        df_words, 
        x='Fréquence', 
        y='Mot', 
        orientation='h', 
        title="Top 20 des mots les plus utilisés",
        color='Fréquence',
        color_continuous_scale='Reds'
    )
    st.plotly_chart(fig_words, use_container_width=True)
else:
    st.write("Pas assez de données pour analyser les mots.")


# --- Etude des appareils utilisés pour tweeter (si cette info est disponible) ---
st.divider()
st.subheader("📱 Appareils et Sources des Tweets")

# Vérification de sécurité
if 'source' in df_filtered.columns:
    import re

    def clean_source(html_source):
        if pd.isna(html_source) or html_source is None:
            return "Unknown"
        # Extrait le texte entre > et <
        match = re.search(r'>(.*?)</a>', str(html_source))
        return match.group(1) if match else str(html_source)

    # Utilisation de .loc pour éviter les warnings de Pandas
    df_filtered = df_filtered.copy() 
    df_filtered['source_clean'] = df_filtered['source'].apply(clean_source)

    # Calcul des stats
    source_counts = df_filtered['source_clean'].value_counts().reset_index()
    source_counts.columns = ['Source', 'Tweets']
    source_counts['Pourcentage'] = (source_counts['Tweets'] / len(df_filtered)) * 100

    col_chart, col_data = st.columns([2, 1])

    with col_chart:
        fig_source = px.pie(
            source_counts.head(8), 
            names='Source', 
            values='Tweets',
            hole=0.5,
            title="Répartition des plateformes",
            template="plotly_dark"
        )
        st.plotly_chart(fig_source, use_container_width=True)

    with col_data:
        st.write("Détails :")
        st.dataframe(
            source_counts[['Source', 'Pourcentage']].head(10).style.format({'Pourcentage': '{:.1f}%'}),
            hide_index=True
        )
else:
    st.error("La colonne 'source' est absente de la base de données Atlas.")
    

    # --- SECTION DÉCISIONNELLE ---
    st.sidebar.divider()
    st.sidebar.subheader("💡 Idées de projets")
    st.sidebar.write("1. **Analyse de sentiment** sur les villes hôtes.")
    st.sidebar.write("2. **Détection de bots** (utilisateurs ultra-actifs).")
    st.sidebar.write("3. **Propagation des rumeurs** via les retweets.")