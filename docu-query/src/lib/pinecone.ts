import { Pinecone, PineconeRecord } from "@pinecone-database/pinecone";
import { downloadFromS3 } from "./s3-server";
import { PDFLoader } from "langchain/document_loaders/fs/pdf";
import md5 from "md5";
import {
  Document,
  RecursiveCharacterTextSplitter,
} from "@pinecone-database/doc-splitter";
import { getEmbeddings } from "./embeddings";
import { convertToAscii } from "./utils";

let pinecone: Pinecone | null = null;

export const getPineconeClient = async () => {
  if (!pinecone) {
    pinecone = new Pinecone({
      apiKey: process.env.PINECONE_API_KEY!, // *
      // environment: process.env.PINECONE_ENVIRONMENT!,
    });
  }
  return pinecone;
};

type PDFPage = {
  pageContent: string;
  metadata: {
    loc: { pageNumber: number };
  };
};

export async function loadS3IntoPinecone(fileKey: string) {
  // 1. Obtain pdf -> download from cloud & read pdf
  // console.log("downloading s3 into file system");
  const file_name = await downloadFromS3(fileKey);
  if (!file_name) {
    throw new Error("could not download from s3");
  }
  const loader = new PDFLoader(file_name);
  const pages = (await loader.load()) as PDFPage[];

  // console.log("Starting step2, step1 completed");
  // 2. Split & Segment the pdf into smaller documents
  // pages = Array(13)
  const documents = await Promise.all(pages.map(prepareDocuments));
  // documents = Array(1000)

  // console.log("Starting step3, step2 completed");
  // 3. Vectorize & embed individual documents
  const vectors = await Promise.all(documents.flat().map(embedDocument));

  // console.log("Starting step4, step3 completed");
  // 4. Upload to Pinecone
  const client = await getPineconeClient();
  const pineconeIndex = client.Index("docu-query");

  console.log("Inserting vectors into pinecone");
  // Different namespace will be present for each document
  // File Key needs to be in all ASCII characters
  // const namespace = convertToAscii(fileKey);
  const request = vectors;
  await pineconeIndex.upsert(request);
  // console.log("Vectors:", vectors);
  console.log("Inserted vectors into pinecone");

  return documents[0];
  // const namespace = pineconeIndex.namespace(convertToAscii(fileKey));
  // await namespace.upsert(vectors);
  // console.log(documents, "docsss");
  // console.log("Inserted vectors into pinecone");

  // return documents[0];
}

async function embedDocument(doc: Document) {
  try {
    const embeddings = await getEmbeddings(doc.pageContent);
    // ID the vector within the PineCone
    const hash = md5(doc.pageContent);

    return {
      id: hash,
      values: embeddings,
      metadata: {
        text: doc.metadata.text,
        pageNumber: doc.metadata.pageNumber, // *
      },
    } as PineconeRecord;
  } catch (error) {
    console.log("error in embedding the document", error);
    throw error;
  }
}

export const truncateStringByBytes = (str: string, bytes: number) => {
  const enc = new TextEncoder();
  return new TextDecoder("utf-8").decode(enc.encode(str).slice(0, bytes));
};

async function prepareDocuments(page: PDFPage) {
  let { pageContent, metadata } = page;
  // replace new line with empty string
  pageContent = pageContent.replace(/\n/g, "");
  // Split the docs/page
  const splitter = new RecursiveCharacterTextSplitter();
  const docs = await splitter.splitDocuments([
    new Document({
      pageContent,
      metadata: {
        pageNumber: metadata.loc.pageNumber,
        text: truncateStringByBytes(pageContent, 36000),
      },
    }),
  ]);
  // console.log("Docs Prepared by splitting & segmenting the pdf", docs);
  return docs;
}
