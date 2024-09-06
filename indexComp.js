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
      range: 'Лист2!6:6' // Измените строку по необходимости, если строка с данными в колонке изменилась
    });

    const day = getFormattedDate();

    const headers = response.data.values ? response.data.values[0] : [];
    const campaignIdColumn = headers.indexOf('ID кампании') + 1;
    const viewsColumn = headers.indexOf('Показы') + 1;
    const clicksColumn = headers.indexOf('Клики') + 1;
    const clicksCartColumn = headers.indexOf('Клик-карзина') + 1;
    const cartOrderColumn = headers.indexOf('Карзина-Заказ') + 1;
    let dayColumn = headers.indexOf(day) + 1;

    if (dayColumn === 0) {
      dayColumn = headers.length + 1;

      // Обновляем заголовок с новой датой в Google Sheets
      headers.push(day);
      await sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID,
        range: 'Лист2!6:6', // Диапазон заголовков
        valueInputOption: 'RAW',
        resource: { values: [headers] } // Добавляем дату в конец
      });

      console.log(`Добавлена новая колонка для даты: ${day}`);
    }

    if (
      campaignIdColumn <= 0
      || viewsColumn <= 0
      || clicksColumn <= 0
      || clicksCartColumn <= 0
      || cartOrderColumn <= 0
      || dayColumn <= 0
    ) {
      throw new Error('Не удалось найти все нужные заголовки');
    }

    return {
      campaignIdColumn,
      viewsColumn,
      clicksColumn,
      clicksCartColumn,
      cartOrderColumn,
      dayColumn
    };
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

function handleApiError(error, retryCount = 0) {
  const maxRetries = 3;
  const retryDelay = 10 * 60 * 1000; // 10 минут в миллисекундах

  if (retryCount >= maxRetries) {
    console.error('Превышено максимальное количество попыток');
    return;
  }

  if (error.response) {
    const status = error.response.status;

    if (status === 429) {
      console.error('Превышен лимит запросов, повтор через 10 минут');
      setTimeout(() => {
        processStatisticsWithRetries(retryCount + 1);
      }, retryDelay);
    } else if (status === 204) {
      console.log('Нет данных для запрашиваемой кампании.');
    } else {
      console.error('Ошибка при запросе:', error.response.data);
      console.error('Повторная попытка через 10 минут');
      setTimeout(() => {
        processStatisticsWithRetries(retryCount + 1);
      }, retryDelay);
    }
  } else {
    console.error('Ошибка сети или сервера:', error.message);
    console.error('Повторная попытка через 10 минут');
    setTimeout(() => {
      processStatisticsWithRetries(retryCount + 1);
    }, retryDelay);
  }
}

async function processStatisticsWithRetries(retryCount = 0) {
  try {
    const campaignIds = await getCompainsById();
    const statistics = await getStatistics(campaignIds);
    const dayFroRes = getFormattedDate();

    const result = statistics.map(stat => ({
      id: stat.advertId,
      day: dayFroRes,
      views: stat.views || 0,
      clicks: stat.clicks || 0,
      clicksCart: stat.clicks > 0 ? `${((stat.atbs / stat.clicks) * 100).toFixed(2)}%` : '0.00%',
      cartOrder: stat.atbs > 0 ? `${((stat.orders / stat.atbs) * 100).toFixed(2)}%` : '0.00%',
      sum: `р.${stat.sum}`
    }));

    console.log('Статистика кампаний:', result);
    await updateGoogleSheet(result);
  } catch (error) {
    handleApiError(error, retryCount);
  }
}

async function processStatistics() {
  try {
    const campaignIds = await getCompainsById();
    const statistics = await getStatistics(campaignIds);
    const dayFroRes = getFormattedDate();

    const result = statistics.map(stat => ({
      id: stat.advertId,
      day: dayFroRes,
      views: stat.views || 0,
      clicks: stat.clicks || 0,
      clicksCart: stat.clicks > 0 ? `${((stat.atbs / stat.clicks) * 100).toFixed(2)}%` : '0.00%',
      cartOrder: stat.atbs > 0 ? `${((stat.orders / stat.atbs) * 100).toFixed(2)}%` : '0.00%',
      sum: `р.${stat.sum}`
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

function getFormattedDate() {
  const rawDate = getYesterdayDate();
  const [year, month, day] = rawDate.split('-');
  return `${day}.${month}.${year.slice(2)}`;
}

// Функция для получения всех данных из таблицы
async function getAllRows() {
  try {
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: 'Лист2!A:ZZ' // Укажите диапазон, который охватывает все ваши данные
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
    const { campaignIdColumn, viewsColumn, clicksColumn, clicksCartColumn, cartOrderColumn, dayColumn } = await getColumnIndexes();

    if (
      !campaignIdColumn
      || !viewsColumn
      || !clicksColumn
      || !clicksCartColumn
      || !cartOrderColumn
      || !dayColumn
    ) {
      throw new Error('Не удалось получить индексы колонок');
    }

    const rows = await getAllRows();

    data.forEach(stat => {
      const rowIndex = rows.findIndex(row => row[campaignIdColumn - 1] == stat.id);
      if (rowIndex !== -1) {
        rows[rowIndex][viewsColumn - 1] = stat.views; // Обновляем показы
        rows[rowIndex][clicksColumn - 1] = stat.clicks; // Обновляем клики
        rows[rowIndex][clicksCartColumn - 1] = stat.clicksCart;
        rows[rowIndex][cartOrderColumn - 1] = stat.cartOrder;
        rows[rowIndex][dayColumn - 1] = stat.sum;
      }
    });

    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range: 'Лист2!A:ZZ',
      valueInputOption: 'RAW',
      resource: { values: rows }
    });

  } catch (error) {
    console.error('Ошибка при обновлении Google Sheets:', error.message);
  }
}

// Запуск процесса каждый день в 00:10
cron.schedule('10 00 * * *', async () => {
  console.log('Запуск задачи каждый день в 00:10');
  const statistics = await processStatistics();
  await updateGoogleSheet(statistics);
});
