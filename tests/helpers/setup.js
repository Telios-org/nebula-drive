const fs = require("fs");
const path = require("path");
const { Drive, Account } = require("../../");
const del = require("del");

module.exports.init = async () => {
  await cleanup();
};

async function cleanup() {
  if (fs.existsSync(path.join(__dirname, "../localDrive"))) {
    await del([path.join(__dirname, "../localDrive")]);
  }

  if (fs.existsSync(path.join(__dirname, "../drive1"))) {
    await del([path.join(__dirname, "../drive1")]);
  }

  if (fs.existsSync(path.join(__dirname, "../drive2"))) {
    await del([path.join(__dirname, "../drive2")]);
  }

  if (fs.existsSync(path.join(__dirname, "../drive"))) {
    await del([path.join(__dirname, "../drive")]);
  }

  if (fs.existsSync(path.join(__dirname, "../peer-drive"))) {
    await del([path.join(__dirname, "../peer-drive")]);
  }
}
