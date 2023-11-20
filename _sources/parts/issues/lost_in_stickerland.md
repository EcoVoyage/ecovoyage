# Lost in Stickerland
## When Custom Matrix Sticker Packs Lead to Uncharted Territory

> Please write me a README with instructions how to create a Matrix sticker pack with Maunium and host it on a GitHub page.

### Matrix Sticker Pack Creation with Maunium and Hosting on GitHub Pages

This README provides step-by-step instructions on how to create a custom Matrix sticker pack using the Maunium Sticker Pack Builder and host it on GitHub Pages.

#### Prerequisites

1. [Git](https://git-scm.com/downloads) and [GitHub account](https://github.com/join)
2. [Node.js](https://nodejs.org/en/download/) installed on your local machine
3. Image files for your stickers in PNG or WEBP format, with a transparent background
4. An active Matrix account (e.g., Element)

#### Step 1: Create a GitHub Repository

1. Log in to your GitHub account
2. Click on the "+" icon in the upper-right corner and select "New repository"
3. Name your repository (e.g., `matrix-sticker-pack`)
4. Choose the repository visibility (public or private)
5. Check the "Add a README file" option
6. Click "Create repository"

#### Step 2: Clone the Repository and Set Up GitHub Pages

1. Clone the repository to your local machine by running:

```
git clone https://github.com/<your-username>/<repository-name>.git
```

2. Navigate to the repository folder:

```
cd <repository-name>
```

3. Create a new branch named `gh-pages`:

```
git checkout -b gh-pages
```

4. Push the new branch to GitHub:

```
git push -u origin gh-pages
```

5. Go to your repository settings on GitHub
6. Scroll down to the "GitHub Pages" section
7. Under "Source", select the `gh-pages` branch and click "Save"

Your GitHub Pages site will be available at `https://<your-username>.github.io/<repository-name>/`.

#### Step 3: Create the Sticker Pack with Maunium Sticker Pack Builder

1. Go to the [Maunium Sticker Pack Builder](https://sticker-importer.maunium.net/builder)
2. Click "Add Sticker" and select the image files for your stickers
3. For each sticker, set a unique "Sticker ID" (e.g., `sticker1`)
4. Enter a "Pack name" and a "Pack ID" (e.g., `MyStickerPack`)
5. Click "Export" and download the generated `.mcfg` file (e.g., `MyStickerPack.mcfg`)

#### Step 4: Add the Sticker Pack to Your GitHub Repository

1. Copy the downloaded `.mcfg` file and your sticker image files to your cloned repository folder
2. Add the files to Git:

```
git add MyStickerPack.mcfg <sticker-image-files>
```

3. Commit the changes:

```
git commit -m "Add sticker pack files"
```

4. Push the changes to the `gh-pages` branch:

```
git push
```

The sticker pack files are now available on your GitHub Pages site.

#### Step 5: Share the Sticker Pack

1. In your Matrix client (e.g., Element), open a chat with someone you'd like to share the sticker pack with
2. Send the link to your sticker pack `.mcfg` file:

```
https://<your-username>.github.io/<repository-name>/MyStickerPack.mcfg
```

The recipient can now import the sticker pack into their Matrix client.

#### Conclusion

You've successfully created a custom Matrix sticker pack with the Maunium Sticker Pack Builder and hosted it on GitHub Pages. Share the pack with your friends and enjoy using your new stickers in Matrix conversations!

> Thanks a lot. Is there a way to import the created sticker pack into the web version of element?

Yes, you can import the created sticker pack into the web version of Element. Follow these steps:

1. Open the Element web app and log in to your account.
2. Click on the sticker picker icon (smiley face) in the bottom right corner of the message input area in any chat room.
3. Click on the gear icon in the top right corner of the sticker picker panel to open the "Sticker Packs" settings.
4. Click on the "Add" button at the bottom of the panel.
5. In the "Add Sticker Pack" dialog, paste the URL of your sticker pack `.mcfg` file:

```
https://<your-username>.github.io/<repository-name>/MyStickerPack.mcfg
```

6. Click "Add Sticker Pack."

Your custom sticker pack should now be available in the sticker picker panel, and you can use the stickers in your conversations.

> When I click the sticker picker I get an empty sticker page and the following error in my chrome console...

It seems like you're experiencing an issue with the Element web app related to the sticker picker. This error may be caused by a missing configuration or a bug in the application. Here are some steps to try and resolve the issue:

1. Clear your browser cache and reload the Element web app. This will ensure you're running the latest version of the app and may resolve any issues related to cached data.

2. Check if the issue persists across different browsers (e.g., Firefox, Safari) or in a private/incognito browsing session. This will help determine if the issue is specific to your current browser session or a more general issue with the Element web app.

3. Make sure your Matrix homeserver has the integration manager configured correctly. The integration manager is responsible for handling sticker packs and other integrations. If you're using the default matrix.org homeserver, the integration manager should be pre-configured. However, if you're using a custom homeserver, you might need to configure the integration manager yourself. You can find more information about setting up an integration manager in the [Matrix documentation](https://matrix-org.github.io/matrix-doc/latest/client-server-api/#widgets).

4. If the issue persists, consider reporting the bug to the Element team. You can do this by opening the Element web app, clicking on your avatar in the top left corner, selecting "Settings," navigating to the "Help & About" tab, and clicking "Report a bug." Provide as much information as possible about the issue, including the error message and steps to reproduce the problem.

In the meantime, you can try using the Element desktop app or a mobile app (available for Android and iOS) to import and use your custom sticker pack. The error you encountered might not occur in these other platforms.

> Can you tell me the correct integration manager configuration when using gitter.im as a home server instead of matrix.org?

Gitter.im does not have a built-in integration manager like matrix.org. To use an integration manager with Gitter.im, you will need to set up your own integration manager or use a third-party one.

Here's how to set up and use the Dimension integration manager with a Gitter.im homeserver:

1. First, you need to have a self-hosted instance of the Dimension integration manager running. You can follow the instructions on the [Dimension GitHub page](https://github.com/turt2live/matrix-dimension) to set up your own instance.

2. After setting up your Dimension instance, open the Element web app and log in with your Gitter.im account.

3. Click on your avatar in the top left corner, then select "Settings."

4. Navigate to the "Advanced" tab. Here, you will find a section called "Integration Manager."

5. Click the "Add" button next to "Integration Managers" and enter the URL of your self-hosted Dimension instance, then click "Add Integration Manager."

6. You should now see your Dimension instance listed as an available integration manager. Click the "Set as default" button to make it the default integration manager for your Gitter.im account.

Now, you should be able to use the sticker picker and other integrations provided by the Dimension integration manager with your Gitter.im homeserver.

Keep in mind that the integration manager configuration is specific to the Element client you are using. If you switch between the web, desktop, or mobile versions of Element, you will need to configure the integration manager for each client separately.

> Can I use vector.im as integration manager?


Yes, you can use the vector.im integration manager (also known as Scalar or Element Integrations) with a custom homeserver. Here's how to configure it:

1. Open the Element web app and log in with your custom homeserver account.

2. Click on your avatar in the top left corner, then select "Settings."

3. Navigate to the "Advanced" tab. Here, you will find a section called "Integration Manager."

4. Click the "Add" button next to "Integration Managers" and enter the URL of the vector.im integration manager, which is typically `https://scalar.vector.im`.

5. Click "Add Integration Manager."

6. You should now see the vector.im (Scalar) instance listed as an available integration manager. Click the "Set as default" button to make it the default integration manager for your custom homeserver account.

Now, you should be able to use the sticker picker and other integrations provided by the vector.im integration manager with your custom homeserver.

Please note that using the vector.im integration manager with a custom homeserver might not always provide full functionality, as certain features might be limited to the matrix.org homeserver or require additional configuration on your custom homeserver. However, for basic integrations like sticker packs, the vector.im integration manager should work well.

> I have set the integration manager to scalar.vector.im. But I'm still experiencingthe issue both on the Element web app and on Element Desktop.

I apologize for the inconvenience you're experiencing. It seems the issue may be related to a bug or a problem with the specific homeserver configuration. The best course of action in this situation is to report the issue to the Element and Gitter.im teams. 

To report the issue to the Element team:

1. Open the Element web app or Element Desktop.
2. Click on your avatar in the top left corner, then select "Settings."
3. Navigate to the "Help & About" tab.
4. Click "Report a bug" and provide as much information as possible about the issue, including the error message and steps to reproduce the problem.

To report the issue to the Gitter.im team, you can create an issue on their GitHub repository:

1. Visit the [Gitter.im GitHub repository](https://github.com/gitterHQ/gitter).
2. Click on the "Issues" tab.
3. Click the "New issue" button.
4. Provide a detailed description of the issue, including the error message and steps to reproduce the problem.

While waiting for a response from the Element or Gitter.im team, you can try using a different Matrix client that supports sticker packs, such as [Nheko Reborn](https://github.com/Nheko-Reborn/nheko) or [Quaternion](https://github.com/quotient-im/Quaternion), to see if the issue persists in those clients. Alternatively, you can consider creating a new account on a different Matrix homeserver, such as matrix.org, to see if the sticker pack functionality works as expected there.