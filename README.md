# FAQ
**Question:** 

Why is this needed?

**Answer:** 

The vast majority of textures in the original PS2 version of MGS2 had dimensions that were not a power of 2, ie 2, 4, 8, 16, 32, 64, 128, 256, 512, ect. 

Early DirectX & OpenGL rendering did not support textures with non-^2 dimensions (referred to as arbitrary dimension textures, or [NPOT Textures](https://www.khronos.org/opengl/wiki/NPOT_Texture)) until OpenGL 2.0's release in 2004 which added the ARB_texture_non_power_of_two extension, well after both ports were created, and GPU's at the time ALSO had to be upgraded to support that new OpenGL feature level. 

To even support the majority of console & PC hardware at the time - the team that handled the ports to the Xbox & PC ran all the textures through an automatic resizer, resizing all the textures' dimensions up to the next power of 2 (for example, a 130x70 texture would be sized up to 256x128), 
which introduced significant JPEG-type artifacting / haloing, blurred (and in some cases outright removed) fine details, and added randomly color lines along the edges of a LOT of the game's textures. 

As the Xbox version of the game provided KojiPro with a DirectX based codebase for all future ports, it was the base version used by Bluepoint for the HD Remaster (hence most of the effect scaling/visual bugs present to this day in the Master Collection originating from the 2002 Xbox port), 
and ultimately all versions of MGS2 released off of the PS2 all have this same issue with its textures.

Presumably, it was simply a forgotten footnote in the game's past development cycle that never got communicated to Bluepoint - even though the textures in the Xbox port suffering from compression artifacting being a well known issue (back in the day, at least.)

- As such, the primary goal of this mod is correcting the compression artifacting/pixelization present with ~85% of ALL textures by re-exporting all 15221 textures from each of the original PS2 versions of MGS2, manually identifying each texture, and fully rebuilding the game's file structure.
- The second primary goal is identifying texturing mistakes made by Bluepoint, such as textures that Bluepoint mistakenly placed on the wrong models, and correcting them as part of my [texture fix compilation](https://www.nexusmods.com/metalgearsolid2mc/mods/52) mod.

    - Example (walls with completely wrong textures used):
 ![Xxewq1c](https://github.com/user-attachments/assets/b6d91b6b-bd74-48ec-86db-82df5afee206)
 ![tM7lu5Q](https://github.com/user-attachments/assets/250be26a-97a0-438c-b6aa-638e4c39d80b)
     - Example 2 (models that swap textures depending on what room/angle you're looking at them from:
       ![u36oka6](https://github.com/user-attachments/assets/017b98c7-959f-4827-816d-13efcf930c17)
       ![OSH37yK](https://github.com/user-attachments/assets/6e12644d-b528-4929-b9fe-f83224111b40)

- A secondary goal of this mod ontop of simply fixing texture compression issues is also fully identifying all textures that were upscaled & remade by Bluepoint in the 2011 HD Collection/remaster. (My current estimate of the number of textures that were updated by Bluepoint sits roughly around 15%.)
  - This allows for the creation of a MGS2 Demastered Edition texture pack - which will fully revert all of the HD remastered textures back to their original PS2 versions.
  - This also adds a tertiary goal of backporting all remastered textures back to both PS2 versions of MGS2 via PCSX2 texture replacements packs.

- Another tertiary goal is also fully identify all the texture changes that was made by Kojima Productions themselves between the original 2001 US release of Sons of Liberty, and the 2003 Substance releases.
  - Several stages (such as the Shell 1 Core, B2 - Computer Room) were identified to have been fully retextured, and this allows for the creation of an OG Sons of Liberty texture pack - which fully returns those retextured areas back to the original state they were in for the 2001 US Sons of Liberty release.
 

-------------

**Question:** 

All these textures appear to be transparent - [#1](https://github.com/dotlessone/MGS2-PS2-Textures/issues/1)

**Answer:** 

That is correct! The PS2 had a different color depth from modern systems, and as a result, fully opaque textures from the PS2 show up as having 50% opacity on PC. 
 - Pixels that have 128 (50%) opacity were actually fully opaque on PS2, pixels that are 102 (40%) opacity were 80% on the PS2, 64 (25%) is 50%, ect.

 - All ports of MGS2 to non-PS2 systems have code that automatically double the opacity level to account for the difference in rendering on other systems. 

  - Stripping opacity outright from a texture / setting it to 100% / fully opaque via photoshop will result in MGS2's lighting engine treating the texture completely different. 

-------------

**Question:** 

What tools are you using for this?

**Answer:**

- File Management:
  - Funduc's Duplicate File Finder
  - Voidtool's Everything

- BP_Asset / Manifest Management:
  - Visual Studio Code
  - Notepad++

- Texturing:
  - Adobe Photoshop 2025
    - Using self-made scripts for proper UV edge padding on export due to a legacy photoshop bug with transparent textures.
  - Adobe Substance 3D Painter
  - Chainner
  - Gimp
  - Nvidia Texture Export Tool
    - Using self-made presets for production quality Kaiser filtered mipmaps.
    - I'm more than happy to share my preset with other modders at request!

- Model Viewers:
  - Blender
  - Autodesk Maya
  - Noesis Model Viewer / Exporter
  - Jayveer's MGS2 Master Collection & PS2 Noesis plugins

- Texture Dumping:
  - PCSX2

- CTXR Generation:
  - 316austin316's CTXR3
  - Jayveer's CTXRTool
    - Using self-made batch scripts for automated mipmap generation using Nvidia's texture tool.

- Other:
  - i2ocr's Japanese Optical Character Recognition
  - Self made tooling to automatically identify remade textures & images that area already ^2.
  - Self made tooling to automatically resize needed images up to the next power of 2. https://github.com/dotlessone/MGS2-PS2-Textures
