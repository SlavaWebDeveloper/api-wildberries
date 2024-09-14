# Свяжитесь со мной @antony_work
require('dotenv').config();
const axios = require('axios');
const fs = require('fs');
const path = require('path');
const AdmZip = require('adm-zip');

const API_TOKEN = process.env.API_TOKEN;
const API_URL = process.env.API_URL;

const api = axios.create({
  baseURL: API_URL,
  headers: {
    'Authorization': API_TOKEN
  }
});

async function getReports() {
  try {
    const response = await api.get('api/v2/nm-report/downloads');
    const reports = response.data.data;
    return reports.map(report => report.id);
  } catch (error) {
    console.error(error.response ? error.response.data : error.message);
    throw error;
  }
}

async function getReport(downloadId, retryCount = 0) {
  try {
    const response = await api.get(`api/v2/nm-report/downloads/file/${downloadId}`, {
      responseType: 'arraybuffer' // Получаем бинарные данные
    });

    const zip = new AdmZip(response.data);
    const zipEntries = zip.getEntries();
    let csvData = '';

    zipEntries.forEach(entry => {
      if (entry.entryName.endsWith('.csv')) {
        csvData = entry.getData().toString('utf8');
      }
    });

    return {
      id: downloadId,
      csvData
    };
  } catch (error) {
    if (error.response && error.response.status === 429) {
      // Если ошибка 429, подождать 70 секунд и повторить запрос
      if (retryCount < 3) { // Лимит на количество повторов
        console.log(`Слишком много запросов. Попробуем снова через 70 секунд. Попытка ${retryCount + 1}`);
        await new Promise(resolve => setTimeout(resolve, 70000));
        return getReport(downloadId, retryCount + 1);
      } else {
        console.error('Превышено количество попыток при получении отчета.');
        throw error;
      }
    } else {
      console.error(error.response ? error.response.data : error.message);
      throw error;
    }
  }
}

async function processReports() {
  try {
    const dataIds = await getReports();
    const reportPromises = dataIds.map(id => getReport(id));
    const reports = await Promise.all(reportPromises);

    reports.forEach(report => {
      const filePath = path.join(__dirname, `${report.id}.csv`);
      fs.writeFileSync(filePath, report.csvData, 'utf8');
      console.log(`CSV файл записан как ${report.id}`);
    });
  } catch (error) {
    console.error('Ошибка при обработке отчетов:', error.message);
  }
}

// Запускаем обработку отчетов
processReports();
