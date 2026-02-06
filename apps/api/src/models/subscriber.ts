import { pool } from "../db";

async function inactivate(chatId: string) {
  const result = await runUpdateQuery(chatId);

  return result;

  async function runUpdateQuery(chatId: string) {
    const query = await pool.query({
      text: `
            UPDATE
                subscribers
            SET 
                active=false
            WHERE 
                chat_id = $1
            RETURNING 
                *
            `,
      values: [chatId],
    });

    if (query.rowCount === 0) {
      throw new Error("Subscriber not found");
    }

    return query.rows[0];
  }
}

async function create(chatId: string) {
  const result = await runInsertIntoQuery(chatId);
  return result;

  async function runInsertIntoQuery(chatId: string) {
    const query = await pool.query({
      text: `
            INSERT INTO 
                subscribers (chat_id)
            VALUES 
                ($1)
            RETURNING 
                *
            `,
      values: [chatId],
    });

    if (query.rowCount === 0) {
      throw new Error("Failed to create subscriber");
    }

    return query.rows[0];
  }
}

const subscriber = {
  inactivate,
  create,
};

export default subscriber;
