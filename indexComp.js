require('dotenv').config();
const { google } = require('googleapis');
const axios = require('axios');
const fs = require('fs');
const cron = require('node-cron');

// Google Sheets credentials
const credentials = JSON.parse(fs.readFileSync('credentials.json'));

const API_TOKEN = process.env.API_TOKEN;
const API_URL = process.env.API_URL_COMPANIES;
const SHEET_ID = process.env.GOOGLE_SHEET_ID;

const api = axios.create({
  baseURL: API_URL,
  headers: {
    'Authorization': API_TOKEN,
    'Content-Type': 'application/json'
  }
});

// Инициализация Google Sheets API
const auth = new google.auth.JWT(
  credentials.client_email,
  null,
  credentials.private_key,
  ['https://www.googleapis.com/auth/spreadsheets'],
  null
);

const sheets = google.sheets({ version: 'v4', auth });

async function getColumnIndexes() {
  try {
    // Укажите строку, где находятся заголовки
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: 'Лист2!6:6' // Измените строку по необходимости
    });

    const headers = response.data.values ? response.data.values[0] : [];

    const campaignIdColumn = headers.indexOf('ID кампании') + 1;
    const viewsColumn = headers.indexOf('Показы') + 1;
    const clicksColumn = headers.indexOf('Клики') + 1;

    if (campaignIdColumn <= 0 || viewsColumn <= 0 || clicksColumn <= 0) {
      throw new Error('Не удалось найти все нужные заголовки');
    }

    return { campaignIdColumn, viewsColumn, clicksColumn };
  } catch (error) {
    console.error('Ошибка при получении заголовков:', error.message);
    throw error;
  }
}

async function getCompains() {
  try {
    const response = await api.post('adv/v1/promotion/adverts?status=9');

    if (response.status === 204) {
      console.log('Кампании не найдены.');
      return [];
    }

    return response.data;
  } catch (error) {
    handleApiError(error);
  }
}

async function getCompainsById() {
  try {
    const compains = await getCompains();
    return compains.map(compain => ({ id: compain.advertId }));
  } catch (error) {
    handleApiError(error);
  }
}

async function getStatistics(campaignIds) {
  try {
    const yesterdayDate = getYesterdayDate();
    const requestBody = campaignIds.map(advertId => ({
      id: advertId.id,
      dates: [yesterdayDate]
    }));

    const response = await api.post('/adv/v2/fullstats', requestBody);
    return response.data;
  } catch (error) {
    handleApiError(error);
  }
}

function handleApiError(error) {
  if (error.response) {
    const status = error.response.status;
    if (status === 429) {
      console.error('Превышен лимит запросов');
      setTimeout(() => {
        processStatistics();
      }, 70000); 
    } else if (status === 204) {
      console.log('Нет данных для запрашиваемой кампании.');
    } else {
      console.error('Ошибка при запросе:', error.response.data);
    }
  } else {
    console.error('Ошибка сети или сервера:', error.message);
  }
}

async function processStatistics() {
  try {
    const campaignIds = await getCompainsById();
    const statistics = await getStatistics(campaignIds);

    const result = statistics.map(stat => ({
      id: stat.advertId,
      day: getYesterdayDate(),
      views: stat.views || 0,
      clicks: stat.clicks || 0
    }));

    console.log('Статистика кампаний:', result);
    return result;
  } catch (error) {
    handleApiError(error);
  }
}

function getYesterdayDate() {
  const today = new Date();
  const yesterday = new Date(today);
  yesterday.setDate(today.getDate() - 1);
  return yesterday.toISOString().split('T')[0]; // Формат YYYY-MM-DD
}

// Функция для получения всех данных из таблицы
async function getAllRows() {
  try {
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: 'Лист2!A:Z' // Укажите диапазон, который охватывает все ваши данные
    });

    return response.data.values || [];
  } catch (error) {
    console.error('Ошибка при получении данных:', error.message);
    throw error;
  }
}

// Функция для обновления Google Sheets
async function updateGoogleSheet(data) {
  try {
    const { campaignIdColumn, viewsColumn, clicksColumn } = await getColumnIndexes();
    
    if (!campaignIdColumn || !viewsColumn || !clicksColumn) {
      throw new Error('Не удалось получить индексы колонок');
    }

    const rows = await getAllRows();

    data.forEach(stat => {
      const rowIndex = rows.findIndex(row => row[campaignIdColumn - 1] == stat.id);
      if (rowIndex !== -1) {
        rows[rowIndex][viewsColumn - 1] = stat.views; // Обновляем показы
        rows[rowIndex][clicksColumn - 1] = stat.clicks; // Обновляем клики
      }
    });

    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range: 'Лист2!A:Z', // Укажите диапазон для обновления
      valueInputOption: 'RAW',
      resource: { values: rows }
    });

    console.log('Таблица обновлена.');
  } catch (error) {
    console.error('Ошибка при обновлении Google Sheets:', error.message);
  }
}

// Запуск процесса каждую минуту
cron.schedule('* * * * *', async () => {
  console.log('Запуск задачи каждую минуту');
  const statistics = await processStatistics();
  await updateGoogleSheet(statistics);
});
